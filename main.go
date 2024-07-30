package main

import (
	"context"
	"fmt"
	"os"

	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/spf13/cobra"
)

var (
	cfg     aws.Config
	profile string
)

func main() {
	var region string

	var rootCmd = &cobra.Command{
		Use:   "aws-tool",
		Short: "AWS operations tool",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			var err error
			if profile != "" {
				cfg, err = config.LoadDefaultConfig(context.TODO(), config.WithRegion(region), config.WithSharedConfigProfile(profile))
			} else if region != "" {
				cfg, err = config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
			} else {
				cfg, err = config.LoadDefaultConfig(context.TODO())
			}
			if err != nil {
				slog.Error("unable to load SDK config", slog.String("err", err.Error()))
			}
			return nil
		},
	}

	rootCmd.PersistentFlags().StringVarP(&region, "region", "r", "", "AWS region (default uses environment configuration)")
	rootCmd.PersistentFlags().StringVarP(&profile, "profile", "p", "", "AWS profile (default uses environment configuration)")

	var s3Cmd = &cobra.Command{
		Use:   "s3-size [bucket name]",
		Short: "Calculate S3 bucket size",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			bucketName := args[0]
			s3Client := s3.NewFromConfig(cfg)

			size, err := calculateS3BucketSize(context.TODO(), s3Client, bucketName)
			if err != nil {
				slog.Error("Failed to calculate bucket size", slog.String("err", err.Error()))
				os.Exit(1)
			}
			fmt.Printf("%d bytes\n", size)
		},
	}

	rootCmd.AddCommand(s3Cmd)

	if err := rootCmd.Execute(); err != nil {
		slog.Error("Error executing command", slog.String("err", err.Error()))
		os.Exit(1)
	}
}

func calculateS3BucketSize(ctx context.Context, client *s3.Client, bucketName string) (int64, error) {
	var totalSize int64
	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	}

	paginator := s3.NewListObjectsV2Paginator(client, params)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return 0, err
		}

		for _, object := range page.Contents {
			totalSize += *object.Size
		}
	}

	return totalSize, nil
}
