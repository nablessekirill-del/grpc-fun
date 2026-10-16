package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	userv1 "grpc1/gen/user/v1"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type userServer struct {
	userv1.UnimplementedUserServiceServer
}

func (s *userServer) GetUser(ctx context.Context, req *userv1.GetUserRequest) (*userv1.GetUserResponse, error) {
	/*
					log.Printf("GetUser: начал id=%d, план работать %d мс", req.Id, req.DelayMs)

					select {
					case <-time.After(time.Duration(req.DelayMs) * time.Millisecond):

						log.Printf("Work is finished: id = %d", req.Id)
						return &userv1.GetUserResponse{Id: req.Id, Name: "Alice"}, nil

					case <-ctx.Done():
						log.Printf("Work is interupted: id = %d, reason: %v", req.Id, ctx.Err())
						return nil, status.FromContextError(ctx.Err()).Err()
					}

				if deadline, ok := ctx.Deadline(); ok {
					log.Printf("клиент отвёл мне времени: %v", time.Until(deadline))
				} else {
					log.Printf("клиент дедлайн не задал")
				}

				log.Printf("GetUser: начал id=%d, план работать %d мс", req.Id, req.DelayMs)
				select {
				case <-time.After(time.Duration(req.DelayMs) * time.Millisecond):
					log.Println("Finished comleted")
					return &userv1.GetUserResponse{Id: req.Id, Name: "Alice"}, nil
				case <-ctx.Done():
					log.Println("Not completed")
					return nil, status.FromContextError(ctx.Err()).Err()
				}

			md, ok := metadata.FromIncomingContext(ctx)

			if ok {
				if auth := md.Get("authorization"); len(auth) > 0 {
					log.Printf("сервер: authorization = %s", auth[0])
				}
				if reqID := md.Get("x-request-id"); len(reqID) > 0 {
					log.Printf("сервер: request-id = %s", reqID[0])
				}
			}
			return &userv1.GetUserResponse{Id: req.Id, Name: "Alice"}, nil

		if s.overloaded() {
			st := status.New(codes.ResourceExhausted, "server overloaded")
			st, _ = st.WithDetails(&errdetails.RetryInfo{
				RetryDelay: durationpb.New(3 * time.Second),
			})
			return nil, st.Err()
		}*/

	if p, ok := peer.FromContext(ctx); ok {
		if tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo); ok {
			certs := tlsInfo.State.PeerCertificates
			if len(certs) > 0 {
				log.Printf("Клиент: CN = %s", certs[0].Subject.CommonName)
			}
		}
	}

	if req.Id <= 0 {
		st := status.New(codes.InvalidArgument, "validation failed")

		badReq := &errdetails.BadRequest{
			FieldViolations: []*errdetails.BadRequest_FieldViolation{
				{
					Field:       "id",
					Description: "must be a positive integer",
				},
			},
		}

		st, err := st.WithDetails(badReq)
		if err != nil {
			return nil, status.Error(codes.Internal, "failed to attach details")
		}

		return nil, st.Err()
	}
	if req.Id == 999 {
		return nil, status.Errorf(codes.NotFound, "user %d not found", req.Id)
	}

	header := metadata.Pairs("x-server-version", "1.0.0")
	grpc.SendHeader(ctx, header)

	trailer := metadata.Pairs("x-processing-time", "42ms")
	grpc.SetTrailer(ctx, trailer)
	return &userv1.GetUserResponse{Id: req.Id, Name: "Alice"}, nil
}

/*
func loggingInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	start := time.Now()

	log.Printf("→ начало: метод=%s", info.FullMethod)

	resp, err := handler(ctx, req)

	log.Printf("← конец: метод=%s, длительность=%v, ошибка=%v", info.FullMethod, time.Since(start), err)

	return resp, err
}*/

func authInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	md, ok := metadata.FromIncomingContext(ctx)

	if !ok {
		return nil, status.Error(codes.Unauthenticated, "no metadata")
	}

	tokens := md.Get("authorization")
	if len(tokens) == 0 {
		return nil, status.Error(codes.Unauthenticated, "no tokens")
	}

	if tokens[0] != "Bearer secret123" {
		return nil, status.Error(codes.Unauthenticated, "no authorization token")
	}

	return handler(ctx, req)
} /*

func recoveryInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (resp any, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("ПАНИКА в %s: %v", info.FullMethod, r)
			err = status.Errorf(codes.Internal, "internal error")
		}
	}()

	return handler(ctx, req)
}

func streamLoggingInterceptor(
	srv any,
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	start := time.Now()
	log.Printf("→ стрим начат: %s", info.FullMethod)

	err := handler(srv, ss)

	log.Printf("← стрим завершён: %s, длительность=%v, ошибка=%v", info.FullMethod, time.Since(start), err)
	return err
} */

// чтобы перехватывать каждое сообщение в стриме. В интерспеторе (в хендлер) передаем обернутый стрим вместо обычного
/*
type wrappedStream struct {
	grpc.ServerStream
}

func (w *wrappedStream) RecvMsg(m any) error {
	err := w.ServerStream.RecvMsg(m)
	log.Printf("  получено сообщение: %v", m)
	return err
}

func (w *wrappedStream) SendMsg(m any) error {
	log.Printf("  отправка сообщения: %v", m)
	return w.ServerStream.SendMsg(m)  // реальная отправка
}*/

func slogAdapter(l *slog.Logger) logging.Logger {
	return logging.LoggerFunc(func(ctx context.Context, lvl logging.Level, msg string, fields ...any) {
		l.Log(ctx, slog.Level(lvl), msg, fields...)
	})
}

func loadServerMTLS() credentials.TransportCredentials {
	serverSert, err := tls.LoadX509KeyPair("certs/mtls/server.crt", "certs/mtls/server.key")
	if err != nil {
		log.Fatal(err)
	}

	caSert, _ := os.ReadFile("certs/mtls/ca.crt")
	caPool := x509.NewCertPool()
	caPool.AppendCertsFromPEM(caSert)

	config := &tls.Config{
		Certificates: []tls.Certificate{serverSert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caPool,
	}
	return credentials.NewTLS(config)
}

func main() {
	kasp := keepalive.ServerParameters{
		Time:                  30 * time.Second,
		Timeout:               5 * time.Second,
		MaxConnectionIdle:     10 * time.Minute,
		MaxConnectionAge:      30 * time.Minute,
		MaxConnectionAgeGrace: 5 * time.Second,
	}

	kaep := keepalive.EnforcementPolicy{
		MinTime:             10 * time.Second,
		PermitWithoutStream: true,
	}

	/*
		creds, err := credentials.NewServerTLSFromFile("server.crt", "server.key")
		if err != nil {
			log.Fatalf("не удалось загрузить TLS: %v", err)
		}*/

	logger := slog.Default()

	lis, err := net.Listen("tcp", ":50051") //Открывает TCP-сокет и начинает слушать входящие соединения на порту 50051
	//Сам по себе net.Listen ничего не обрабатывает — просто резервирует порт
	// и готовит очередь входящих подключений. Приёмом и обработкой занимается уже Serve.
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	opts := []recovery.Option{
		recovery.WithRecoveryHandler(func(p any) error {
			log.Printf("паника: %v", p)
			return status.Errorf(codes.Internal, "internal error")
		}),
	}

	grpcServer := grpc.NewServer(
		grpc.KeepaliveParams(kasp),
		grpc.KeepaliveEnforcementPolicy(kaep),
		//grpc.Creds(creds),
		grpc.Creds(loadServerMTLS()),

		//grpc.UnaryInterceptor(loggingInterceptor),
		//grpc.UnaryInterceptor(authInterceptor),
		grpc.ChainUnaryInterceptor(
			recovery.UnaryServerInterceptor(opts...),
			logging.UnaryServerInterceptor(slogAdapter(logger)),
			authInterceptor,
		),
		grpc.ChainStreamInterceptor(
			recovery.StreamServerInterceptor(),
			logging.StreamServerInterceptor(slogAdapter(logger)),
		),
	) //Создаёт объект gRPC-сервера — ядро, которое умеет обрабатывать gRPC-запросы.
	//содержит пустую таблицу маршрутизации (сервисы будут добавлены через Register...)
	//настройки HTTP/2, лимиты, кодеки,
	//механизм запуска горутины на каждый входящий RPC.
	//Сам по себе созданный сервер ничего не слушает и не обрабатывает
	// — он «спит», пока не вызовешь Serve(lis) с listener'ом.

	userv1.RegisterUserServiceServer(grpcServer, &userServer{}) //Регистрирует твою реализацию сервиса в сервере —
	//кладёт в его таблицу маршрутизации связку «путь запроса → твоя функция»
	//&userServer{} — что регистрируем: указатель на экземпляр твоей структуры с методами (GetUser и т.д.).
	//
	go func() {
		if err := grpcServer.Serve(lis); err != nil { //Запускает сервер: начинает принимать
			//соединения через listener и обслуживать RPC
			log.Fatalf("failed to serve: %v", err)
		}
	}()
	log.Println("listening on :50051")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM) //Подписывается на сигналы операционной системы — говорит Go:
	// «когда придёт SIGINT (os.Interrupt) или SIGTERM, положи его в канал stop»
	<-stop

	log.Println("Shutting down")

	grpcServer.GracefulStop() //Корректно останавливает сервер: перестаёт принимать новое, но даёт доработать текущему
	log.Println("Stopped")
}
