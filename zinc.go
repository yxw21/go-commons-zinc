package go_commons_zinc

import (
	"context"
	zinc "github.com/zinclabs/sdk-go-zincsearch"
	"net/http"
	"os"
)

type Client struct {
	index   string
	context context.Context
	zinc    *zinc.APIClient
}

func (ctx *Client) SetIndex(index string) {
	ctx.index = index
}

func (ctx *Client) SetAuth(username, password string) {
	ctx.context = context.WithValue(context.Background(), zinc.ContextBasicAuth, zinc.BasicAuth{
		UserName: username,
		Password: password,
	})
}

func (ctx *Client) SetEndpoint(endpoint string) {
	configuration := zinc.NewConfiguration()
	configuration.Servers = zinc.ServerConfigurations{
		zinc.ServerConfiguration{
			URL: endpoint,
		},
	}
	ctx.zinc = zinc.NewAPIClient(configuration)
}

func (ctx *Client) getIndex(args ...string) string {
	if len(args) > 0 {
		return args[0]
	}
	return ctx.index
}

func (ctx *Client) DefaultHealthZ() (*zinc.MetaHealthzResponse, *http.Response, error) {
	return ctx.zinc.Default.Healthz(context.Background()).Execute()
}

func (ctx *Client) DefaultVersion() (*zinc.MetaVersionResponse, *http.Response, error) {
	return ctx.zinc.Default.Version(context.Background()).Execute()
}

func (ctx *Client) DocumentBulk(query string) (*zinc.MetaHTTPResponseRecordCount, *http.Response, error) {
	return ctx.zinc.Document.Bulk(ctx.context).Query(query).Execute()
}

func (ctx *Client) DocumentBulkV2(records []map[string]interface{}, args ...string) (*zinc.MetaHTTPResponseRecordCount, *http.Response, error) {
	query := zinc.NewMetaJSONIngest()
	query.SetIndex(ctx.getIndex(args...))
	query.SetRecords(records)
	return ctx.zinc.Document.Bulkv2(ctx.context).Query(*query).Execute()
}

func (ctx *Client) DocumentDelete(id string, args ...string) (*zinc.MetaHTTPResponseDocument, *http.Response, error) {
	return ctx.zinc.Document.Delete(ctx.context, ctx.getIndex(args...), id).Execute()
}

func (ctx *Client) DocumentESBulk(query string) (map[string]interface{}, *http.Response, error) {
	return ctx.zinc.Document.ESBulk(ctx.context).Query(query).Execute()
}

func (ctx *Client) DocumentIndex(document map[string]interface{}, args ...string) (*zinc.MetaHTTPResponseID, *http.Response, error) {
	return ctx.zinc.Document.Index(ctx.context, ctx.getIndex(args...)).Document(document).Execute()
}

func (ctx *Client) DocumentIndexWithID(id string, document map[string]interface{}, args ...string) (*zinc.MetaHTTPResponseID, *http.Response, error) {
	return ctx.zinc.Document.IndexWithID(ctx.context, ctx.getIndex(args...), id).Document(document).Execute()
}

func (ctx *Client) DocumentMulti(query string, args ...string) (*zinc.MetaHTTPResponseRecordCount, *http.Response, error) {
	return ctx.zinc.Document.Multi(ctx.context, ctx.getIndex(args...)).Query(query).Execute()
}

func (ctx *Client) DocumentUpdate(id string, document map[string]interface{}, args ...string) (*zinc.MetaHTTPResponseID, *http.Response, error) {
	return ctx.zinc.Document.Update(ctx.context, ctx.getIndex(args...), id).Document(document).Execute()
}

func (ctx *Client) ESCreateIndex(args ...string) (map[string]interface{}, *http.Response, error) {
	return ctx.zinc.Index.ESCreateIndex(ctx.context, ctx.getIndex(args...)).Execute()
}

func (ctx *Client) IndexExists(args ...string) (*zinc.MetaHTTPResponse, *http.Response, error) {
	return ctx.zinc.Index.Exists(ctx.context, ctx.getIndex(args...)).Execute()
}

func (ctx *Client) IndexGetMapping(args ...string) (map[string]interface{}, *http.Response, error) {
	return ctx.zinc.Index.GetMapping(ctx.context, ctx.getIndex(args...)).Execute()
}

func (ctx *Client) GetContext() context.Context {
	return ctx.context
}

func (ctx *Client) GetZinc() *zinc.APIClient {
	return ctx.zinc
}

func NewClient(index string) *Client {
	instance := &Client{}
	instance.SetEndpoint(os.Getenv("ZINC_ENDPOINT"))
	instance.SetAuth(os.Getenv("ZINC_USERNAME"), os.Getenv("ZINC_PASSWORD"))
	instance.SetIndex(index)
	return instance
}
