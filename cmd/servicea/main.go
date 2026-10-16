package main

import (
	"context"
	userv1 "grpc1/gen/user/v1"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type serviceA struct {
	userv1.UnimplementedUserServiceServer
	bClient userv1.UserServiceClient
}

func (a *serviceA) GetUser(ctx context.Context, req *userv1.GetUserRequest) (*userv1.GetUserResponse, error) {

	subCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	// if d, ok := ctx.Deadline(); ok {
	// 	log.Printf("A: клиент отвел мне %v", time.Until(d))
	// }
	//log.Println("A: Вызываю В и пробрасываю тот же контекст")
	//log.Println("A: Вызываю В и пробрасываю плохой контекст")

	//badCtx := context.Background()

	resp, err := a.bClient.GetUser(subCtx, req)
	if err != nil {
		if status.Code(err) == codes.DeadlineExceeded {
			log.Println("A: B не успел, отдаю fallback")
			return &userv1.GetUserResponse{Id: req.Id, Name: "default"}, nil
		}
		return nil, err
	}
	return resp, nil
}

func main() {
	coonB, _ := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer coonB.Close()

	a := &serviceA{bClient: userv1.NewUserServiceClient(coonB)}

	lis, _ := net.Listen("tcp", ":50052")
	srv := grpc.NewServer()
	userv1.RegisterUserServiceServer(srv, a)
	log.Println("service A listening on :50052")
	srv.Serve(lis)

}
