package httpd

import (
	"context"
	"fmt"
	"github.com/influxdata/influxdb/prometheus"
	"github.com/influxdata/influxdb/prometheus/remote"
	"github.com/influxdata/influxdb/tsdb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
	"strings"
	"time"
)

type Server struct {
	remote.UnimplementedQueryTimeSeriesServiceServer
	Store Store
}

func (s *Server) Raw(req *remote.FilterRequest, stream remote.QueryTimeSeriesService_RawServer) error {
	readRequest, err := GetReadRequest(req.GetDb(), req.GetRp(), req.GetMeasurement(), req.GetField(), req.GetWhere())
	if err != nil {
		return err
	}

	ctx := context.Background()
	rs, err := s.Store.ReadFilter(ctx, readRequest)
	if err != nil {
		return err
	}
	if rs != nil {
		defer rs.Close()
	}

	var (
		seriesNum int64 = 0
		pointsNum int64 = 0
	)

	for rs.Next() {
		cur := rs.Cursor()
		if cur == nil {
			continue
		}

		tags := prometheus.RemoveInfluxSystemTags(rs.Tags())
		series := &remote.TimeSeries{
			Labels: prometheus.ModelTagsToLabelPairs(tags),
		}

		if req.GetSlimit() > 0 && seriesNum >= req.GetSlimit() {
			return nil
		}
		if req.GetLimit() > 0 && pointsNum >= req.GetLimit() {
			return nil
		}

		var unsupportedCursor string
		switch cur := cur.(type) {
		case tsdb.FloatArrayCursor:
			for {
				a := cur.Next()
				if a.Len() == 0 {
					goto ADD
				}

				for i, ts := range a.Timestamps {
					pointsNum++
					series.Samples = append(series.Samples, &remote.Sample{
						TimestampMs: ts / int64(time.Millisecond),
						Value:       a.Values[i],
					})

					if req.GetLimit() > 0 && pointsNum >= req.GetLimit() {
						goto ADD
					}
				}
			}
		case tsdb.IntegerArrayCursor:
			for {
				a := cur.Next()
				if a.Len() == 0 {
					goto ADD
				}

				for i, ts := range a.Timestamps {
					pointsNum++
					series.Samples = append(series.Samples, &remote.Sample{
						TimestampMs: ts / int64(time.Millisecond),
						Value:       float64(a.Values[i]),
					})

					if req.GetLimit() > 0 && pointsNum >= req.GetLimit() {
						goto ADD
					}
				}
			}
		case tsdb.UnsignedArrayCursor:
			unsupportedCursor = "uint"
		case tsdb.BooleanArrayCursor:
			unsupportedCursor = "bool"
		case tsdb.StringArrayCursor:
			unsupportedCursor = "string"
		default:
			return fmt.Errorf("unreachable: %T", cur)
		}
		cur.Close()

		if len(unsupportedCursor) > 0 {
			return fmt.Errorf("raw can't read cursor, cursor_type: %s, series: %s", unsupportedCursor, tags)
		}
	ADD:
		if len(series.Samples) > 0 {
			seriesNum++
			stream.Send(series)
		}
	}

	return nil
}

type RpcService struct {
	addr   string
	ln     net.Listener
	serv   *grpc.Server
	logger *zap.Logger

	server *Server

	err chan error
}

func NewGrpcService(address string, store Store) *RpcService {
	serv := &RpcService{
		addr:   address,
		serv:   grpc.NewServer(),
		err:    make(chan error),
		logger: zap.NewNop(),
		server: &Server{
			Store: store,
		},
	}
	return serv
}

func (s *RpcService) WithLogger(log *zap.Logger) {
	s.logger = log.With(zap.String("service", "grpc"))
}

func (s *RpcService) Close() error {
	s.serv.GracefulStop()
	return nil
}

func (s *RpcService) Open() error {
	s.logger.Info("Starting GRPC service")
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("unable to listen on port %s: %v", s.addr, err)
	}
	s.ln = lis
	s.logger.Info("Listening on GRPC",
		zap.Stringer("addr", s.ln.Addr()),
	)

	// Begin listening for requests in a separate goroutine.
	go s.serve()
	return nil
}

// serve serves the handler from the listener.
func (s *RpcService) serve() {
	remote.RegisterQueryTimeSeriesServiceServer(s.serv, s.server)
	reflection.Register(s.serv)

	if err := s.serv.Serve(s.ln); err != nil && !strings.Contains(err.Error(), "closed") {
		s.err <- fmt.Errorf("listener failed: addr=%s, err=%s", s.ln.Addr(), err)
	}
}
