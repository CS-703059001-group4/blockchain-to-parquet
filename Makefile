DATE := "2018-01-01"

index:
	@echo "Index transaction outputs"
	@go run cmd/indexer/main.go
	@echo ""

convert:
	@echo "Convert dat file to parquet format"
	@go run cmd/converter/main.go
	@echo ""

finddat:
	@go run cmd/finddat/main.go -by=date -data=/Volumes/Transcend/bitcoin-data/Bitcoin/blocks $(DATE)

clean:
	@rm -rf db out