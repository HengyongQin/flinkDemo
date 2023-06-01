package flink.api.syn.operator;

import flink.api.syn.constant.PropertiesConstant;
import flink.api.syn.pojo.EsRow;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch.util.RetryRejectedExecutionFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch7.RestClientFactory;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;
import java.util.List;

/**
 * 创建 es sink
 */
public class EsSinkBuilder {
    private EsSinkBuilder() {
    }

    public static ElasticsearchSink<EsRow> build(ParameterTool param) {
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost(param.get(PropertiesConstant.ES_HOST), param.getInt(PropertiesConstant.ES_PORT), "http"));
        ElasticsearchSink.Builder<EsRow> esSinkBuilder = new ElasticsearchSink.Builder<>(httpHosts, new EsSinkFunction());
        esSinkBuilder.setBulkFlushMaxActions(1000);
        esSinkBuilder.setBulkFlushInterval(1000);  //每秒刷新一次
        esSinkBuilder.setFailureHandler(new RetryRejectedExecutionFailureHandler());
        esSinkBuilder.setRestClientFactory(new EsClientFactory(param.get(PropertiesConstant.ES_USERNAME), param.get(PropertiesConstant.ES_PASS)));
        return esSinkBuilder.build();
    }

    /**
     * 客户端连接
     */
    private static class EsClientFactory implements RestClientFactory {
        private String username;
        private String pass;

        private EsClientFactory(String username, String pass) {
            this.username = username;
            this.pass = pass;
        }

        @Override
        public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(username, pass));
            restClientBuilder.setDefaultHeaders(new BasicHeader[]{new BasicHeader("Content-Type", "application/json")}); //以数组的形式可以添加多个header
            restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> {
                httpClientBuilder.disableAuthCaching();
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            });
            restClientBuilder.setRequestConfigCallback(builder -> builder.setConnectTimeout(1000)
                    .setSocketTimeout(1000)
                    .setConnectionRequestTimeout(0));
        }
    }

    /**
     * es 写操作
     */
    private static class EsSinkFunction implements ElasticsearchSinkFunction<EsRow> {
        @Override
        public void process(EsRow esRow, RuntimeContext runtimeContext, RequestIndexer indexer) {
            String index = esRow.getIndex();
            String id = esRow.getId();

            switch (esRow.getOptType()) {
                case INSERT:
                case UPDATE:
                    indexer.add(Requests.indexRequest(index).id(id).source(esRow.getData(), XContentType.JSON));
                    break;
                case DELETE:
                    indexer.add(Requests.deleteRequest(index).id(id));
            }
        }
    }
}
