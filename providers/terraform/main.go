package main

import (
	"context"
	"github.com/araditc/Kafdeck/providers/terraform/internal/provider"
	"github.com/hashicorp/terraform-plugin-framework/providerserver"
	"log"
)

var version = "0.8.0"

func main() {
	if err := providerserver.Serve(context.Background(), provider.New(version), providerserver.ServeOpts{Address: "registry.terraform.io/araditc/kafdeck", ProtocolVersion: 6}); err != nil {
		log.Fatal(err)
	}
}
