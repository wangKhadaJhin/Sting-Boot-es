package com.wang.es.elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wang.es.elasticsearch.vo.WangyeCeshi;
import org.apache.http.HttpHost;
import org.apache.lucene.codecs.TermVectorsReader;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.TermVectorsRequest;
import org.elasticsearch.client.core.TermVectorsResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.Test;


import java.io.IOException;

/**
 * @author mingsheng.wang
 **/
public class EsTest extends BaseTest{

    @Test
    public void insert() throws IOException {
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost("127.0.0.1",9200,"http")));
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject();
        {xContentBuilder.field("name","wang");
            xContentBuilder.field("sex","man");
        }
        xContentBuilder.endObject();
        IndexRequest indexRequest = new IndexRequest("wangceshi").source(xContentBuilder);
        try {
            IndexResponse indexResponse =restHighLevelClient.index(indexRequest,RequestOptions.DEFAULT);
            System.out.println("返回数据：{}"+indexResponse);
            restHighLevelClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void get() throws IOException {
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost",9200,"http")));

        GetRequest request = new GetRequest("wangceshi","Sqv1A3IB2rZQSn2EMfoh");
        String[] includes = Strings.EMPTY_ARRAY;
        String[] excludes = new String[]{"name"};
        FetchSourceContext fetchSourceContext =
                new FetchSourceContext(true, includes, excludes);
        request.fetchSourceContext(fetchSourceContext);
        GetResponse response =restHighLevelClient.get(request,RequestOptions.DEFAULT);
        System.out.println(response);
    }

    @Test
    public void asyGet() throws IOException {
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost",9200,"http")));

        GetRequest getRequest = new GetRequest("wangceshi","9wk2A3IBAN5EMkUIDU-J");
        ActionListener<GetResponse> listener = new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                System.out.println(getResponse);
            }

            @Override
            public void onFailure(Exception e) {

            }
        };
        restHighLevelClient.getAsync(getRequest,RequestOptions.DEFAULT,listener);
    }

    @Test
    public void update() throws IOException {
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost",9200,"http")));

        UpdateRequest request = new UpdateRequest("wangceshi","9wk2A3IBAN5EMkUIDU-J").doc("name","wang",
                "age","18",
                "sex","man").docAsUpsert(true);
        UpdateResponse updateResponse= restHighLevelClient.update(request,RequestOptions.DEFAULT);
        System.out.println(updateResponse);

    }

    /**
     * 术语向量
     * @throws IOException
     */
    @Test
    public void term() throws IOException {
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost",9200,"http")));
        TermVectorsRequest termVectorsRequest = new TermVectorsRequest("wangceshi","Sqv1A3IB2rZQSn2EMfoh");
        termVectorsRequest.setFields("name");
        TermVectorsResponse response =restHighLevelClient.termvectors(termVectorsRequest,RequestOptions.DEFAULT);
        for (TermVectorsResponse.TermVector termVector : response.getTermVectorsList()){

        }
        System.out.println(response);
    }


    /**
     * 批量操作
     */
    @Test
    public void bulk() throws IOException {
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost("127.0.0.1",9200,"http")));
        BulkRequest bulkRequest  = new BulkRequest();
        WangyeCeshi wangyeCeshi = new WangyeCeshi();
        wangyeCeshi.setName("wang");
        wangyeCeshi.setAge(19);
        wangyeCeshi.setAdress("China Beijing");
        wangyeCeshi.setRemark("remark");
        wangyeCeshi.setSex("man");
        ObjectMapper objectMapper = new ObjectMapper();
        byte [] json = objectMapper.writeValueAsBytes(wangyeCeshi);
        bulkRequest.add(new IndexRequest("wangye").source(json, XContentType.JSON));
        wangyeCeshi.setName("wangye");
        wangyeCeshi.setAge(18);
        ObjectMapper mapper = new ObjectMapper();
        byte [] j = mapper.writeValueAsBytes(wangyeCeshi);
        bulkRequest.add(new IndexRequest("wangye").source(j, XContentType.JSON));
        BulkResponse bulkItemResponses =restHighLevelClient.bulk(bulkRequest,RequestOptions.DEFAULT);
        for (BulkItemResponse bulkItemResponse :bulkItemResponses){
            System.out.println(bulkItemResponse.getResponse());

        }
    }
}
