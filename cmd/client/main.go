package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	userv1 "grpc1/gen/user/v1"
	"log"
	"os"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)


/*
func clientLoggingInterceptor(
	ctx context.Context,
	method string,
	req, reply any,
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	start := time.Now()

	reqID := uuid.NewString()
	ctx = metadata.AppendToOutgoingContext(ctx, "x-request-id", reqID)
	log.Printf("клиент → %s (request-id=%s)", method, reqID)

	err := invoker(ctx, method, req, reply, cc, opts...)

	log.Printf("клиент ← %s, длительность=%v, ошибка=%v",
		method, time.Since(start), err)

	return err

}

func streamClientInterceptor(
	ctx context.Context,
	desc *grpc.StreamDesc,
	cc *grpc.ClientConn,
	method string,
	streamer grpc.Streamer,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	log.Printf("клиент: открываю стрим %s", method)

	clientStream, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil {
		return nil, err
	}

	return clientStream, nil
}*/

func loadServerMTLS() credentials.TransportCredentials {
	clientCert, _ := tls.LoadX509KeyPair("certs/mtls/client.crt", "certs/mtls/client.key")

	caCert, _ := os.ReadFile("certs/mtls/ca.crt")
	caPool := x509.NewCertPool()
	caPool.AppendCertsFromPEM(caCert)

	config := &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      caPool,
		ServerName:   "localhost",
	}
	return credentials.NewTLS(config)
}

type tokenAuth struct {
	token string
}

func (t tokenAuth) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{
		"autorization": "Bearer" + t.token,
	}, nil
}

func (t tokenAuth) RequireTransportSecurity() bool {
	return true
}

func main() {
	kacp := keepalive.ClientParameters{
		Time:                10 * time.Second,
		Timeout:             3 * time.Second,
		PermitWithoutStream: true,
	}

	/*creds, err := credentials.NewClientTLSFromFile("server.crt", "")
	if err != nil {
		log.Fatalf("Не удалось загрузить сертификат: %v", err)
	}*/

	conn, err := grpc.NewClient( //Создаёт клиентское соединение до gRPC-сервера — объект *grpc.ClientConn,
		//через который потом делаются вызовы.
		"localhost:50051", //target ("localhost:50051") — адрес сервера. Формально это строка с resolver-схемой;
		//localhost:50051 неявно превращается в dns:///localhost:50051.
		grpc.WithTransportCredentials(loadServerMTLS()), //опции: WithTransportCredentials обязательна: задаёт транспорт.
		//insecure.NewCredentials() = без TLS (только для локалки/доверенной сети); для прода — TLS-креденшелы.
		//grpc.WithUnaryInterceptor(clientLoggingInterceptor),
		grpc.WithPerRPCCredentials(tokenAuth{token: "seret123"}),
		grpc.WithKeepaliveParams(kacp),
	)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	client := userv1.NewUserServiceClient(conn) //сгенеренная функция (лежит в user_grpc.pb.go) создаёт лёгкую структуру,
	// которая просто хранит ссылку на conn.
	// У неё есть методы с точными сигнатурами из твоего .proto — GetUser(ctx, req) (*GetUserResponse, error) и т.д.

	/*
				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				defer cancel()

			ctx, cancel := context.WithCancel(context.Background())

			go func() {
				time.Sleep(500 * time.Millisecond)
				log.Println("Client canceled req")
				cancel()
			}()


				resp, err := client.GetUser(ctx, &userv1.GetUserRequest{Id: 1, DelayMs: 1000})
				if err != nil {
					switch status.Code(err) { //вытаскивает gRPC-код из ошибки. Это правильный способ реагировать на ошибки — по коду, а не по тексту.
					case codes.DeadlineExceeded:
						log.Println("не успели за отведённое время — сервис тормозит")
					case codes.Canceled:
						log.Println("вызов отменён")
					default:
						log.Printf("другая ошибка: %v", err)
					}
					return
				}


		ctx, cancel := context.WithTimeout(context.Background(), 5000*time.Millisecond)
		defer cancel()

		_, err = client.GetUser(ctx, &userv1.GetUserRequest{Id: 1, DelayMs: 1000})
		if err != nil {
			log.Printf("клиент: ошибка %v (%v)", status.Code(err), err)
			return
		}
		log.Println("ок")
	*/

	md := metadata.New(map[string]string{
		"authorization": "Bearer token123",
		"x-request-id":  "req-42",
	})

	ctx := metadata.NewOutgoingContext(context.Background(), md)

	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	ctx = metadata.AppendToOutgoingContext(context.Background(),
		"authorization", "Bearer secret123")

	var header, trailer metadata.MD

	resp, err := client.GetUser(ctx, &userv1.GetUserRequest{Id: 1, DelayMs: 100},
		grpc.Header(&header),
		grpc.Trailer(&trailer))

	if err != nil {
		//log.Fatalf("error: %v", err)
		/*
			st, ok := status.FromError(err)
			if ok {
				log.Printf("код: %s, сообщение %s", st.Code(), st.Message())
				switch st.Code() {
				case codes.InvalidArgument:
					log.Println("проверь входные данные")
				case codes.NotFound:
					log.Println("→ пользователь не найден")

				case codes.Unavailable:
					log.Println("→ можно повторить позже")
				}
			}
			return
		*/
		st := status.Convert(err)
		log.Printf("код: %s, сообщение: %s", st.Code(), st.Message())
		for _, d := range st.Details() {
			switch info := d.(type) {
			case *errdetails.BadRequest:
				for _, v := range info.GetFieldViolations() {
					log.Printf(" поле %q невалидно: %s", v.GetField(), v.GetDescription())
				}
			case *errdetails.RetryInfo:
				log.Printf("  повторить через: %v", info.GetRetryDelay().AsDuration())
			default:
				log.Printf("  прочая деталь: %v", info)
			}
		}
		return

	}

	log.Printf("Получили: %s", resp.Name)
	if v := header.Get("x-server-version"); len(v) > 0 {
		log.Printf("Version of server: %s", v[0])
	}
	if v := trailer.Get("x-processing-time"); len(v) > 0 {
		log.Printf("время обработки: %s", v[0])
	}

}
