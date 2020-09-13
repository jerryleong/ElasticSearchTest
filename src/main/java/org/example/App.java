package org.example;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders.*;
import org.elasticsearch.index.query.TermsQueryBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.termsQuery;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {

        System.out.println( "Hello World!" );
        try {
            RestHighLevelClient client = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost("192.168.31.83", 9200, "http"),
                            new HttpHost("192.168.31.83", 9201, "http")));
            IndexRequest request = new IndexRequest("posts");
            request.id("2");
            String jsonString = "{" +
                    "\"user\":\"jerry\"," +
                    "\"postDate\":\"2013-01-30\"," +
                    "\"message\":\"trying out Elasticsearch\"" +
                    "}";
            request.source(jsonString, XContentType.JSON);

            IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);

            client.close();
            String permission = "in-OFFICIAL-1-3-9,in-OFFICIAL-1-3-12,in-OFFICIAL-1-3-10,out-OFFICIAL-1-3-9";
            BoolQueryBuilder queryBuilderTop = new BoolQueryBuilder();

            BoolQueryBuilder queryBuilderMust1 = new BoolQueryBuilder();
            BoolQueryBuilder queryBuilderMust2 = new BoolQueryBuilder();
            BoolQueryBuilder queryBuilderShould1 = new BoolQueryBuilder();

            BoolQueryBuilder queryBuilderEmpty = new BoolQueryBuilder();

            queryBuilderMust1.must(termQuery("user", "jerry")) ;
            queryBuilderMust1.must(termQuery("postDate", "2013-01-30")) ;

            queryBuilderMust2.must(termQuery("user", "kimchy"));

            queryBuilderShould1.should(queryBuilderMust1).should(queryBuilderMust2);

            queryBuilderShould1.should(queryBuilderEmpty);

            queryBuilderTop.filter(queryBuilderShould1);


            System.out.println("queryBuilderEmpty hasClauses :" + queryBuilderEmpty.hasClauses());

            System.out.println(queryBuilderTop.toString());

        }catch(IOException e){
            e.printStackTrace();
        }catch(ElasticsearchException e){
            e.printStackTrace();
        }




    }

    private static void generateEsFilterByPermission(int entityTypeId, int entityId, String permissions){
        String permission = "in-OFFICIAL-1-3-9,in-OFFICIAL-1-3-12,in-OFFICIAL-1-3-10,out-OFFICIAL-1-3-9";
        List<String> permissionList = Arrays.asList(permission.split("\\s*,\\s*"));
        List<String[]> opList = permissionList.stream().map(s -> s.split("-", 3))
                .collect(Collectors.toList());

        //for (String[] a : opList){
        //    System.out.println(a[0] + " " +  a[1] + " "  +  a[2] );
        //}
        //List<List<String>> = permissionList.stream().map()
        permissionList.stream().map(s -> s.toLowerCase().split("-", 3))
                .filter(arr -> arr[2].equals("all")
                        || arr[2].contains(Integer.toString(entityTypeId) + "-" + Integer.toString(entityTypeId) + "-") )
                .forEach(arr -> {
                    if("in".equals(arr[0]))
                        System.out.println("IN");
                    if("out".equals(arr[0]))
                        System.out.println("OUT");
                    if("official".equals(arr[1]))
                        System.out.println("Handle Official");
                    if("official".equals(arr[1]))
                        System.out.println("Handle Other");
                    if(!"all".equals(arr[2]))
                        System.out.println("Add Filter " + arr[2]);
                });
    }

    private static BoolQueryBuilder convertPermissionToQuery(String[] permission){
        BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
        if("in".equals(permission[0])){
            queryBuilder.must(termsQuery("itemTransferStatus", "RECEIVED"));
            queryBuilder.must(termsQuery("itemStatus", "PUBLISHED"));
        }
        if("out".equals(permission[0])){
            queryBuilder.must(termsQuery("itemTransferStatus", "SENT", "DRAFT"));
            queryBuilder.must(termsQuery("itemStatus", "PUBLISHED"));
        }
        if("official".equals(permission[1])){
            queryBuilder.must(termsQuery("itemType", "OFFICIAL", "OFFICIAL_CIR"));
        }
        if("other".equals(permission[1])){
            queryBuilder.must(termsQuery("itemType", "OTHER"));
        }

        return queryBuilder;
    }

}

