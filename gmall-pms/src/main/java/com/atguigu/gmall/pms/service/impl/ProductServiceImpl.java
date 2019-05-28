package com.atguigu.gmall.pms.service.impl;

import com.alibaba.dubbo.config.annotation.Service;
import com.atguigu.gmall.constant.EsConstant;
import com.atguigu.gmall.pms.entity.*;
import com.atguigu.gmall.pms.mapper.*;
import com.atguigu.gmall.pms.service.ProductService;
import com.atguigu.gmall.to.es.EsProduct;
import com.atguigu.gmall.to.es.EsProductAttributeValue;
import com.atguigu.gmall.to.es.EsSkuProductInfo;
import com.atguigu.gmall.vo.PageInfoVo;
import com.atguigu.gmall.vo.product.PmsProductParam;
import com.atguigu.gmall.vo.product.PmsProductQueryParam;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import io.searchbox.client.JestClient;
import io.searchbox.core.*;
import lombok.extern.log4j.Log4j;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * <p>
 * 商品信息 服务实现类
 * </p>
 *
 * @author zs
 * @since 2019-05-09
 */
@Slf4j
@Service
@Component
public class ProductServiceImpl extends ServiceImpl<ProductMapper, Product> implements ProductService {

    @Autowired
    ProductMapper
            productMapper;

    @Autowired
    ProductAttributeValueMapper productAttributeValueMapper;

    @Autowired
    ProductFullReductionMapper productFullReductionMapper;

    @Autowired
    ProductLadderMapper productLadderMapper;

    @Autowired
    SkuStockMapper skuStockMapper;

    @Autowired
    JestClient jestClient;

    //当前线程共享同样的数据
    ThreadLocal<Long> threadLocal = new ThreadLocal<>();

    //ThreadLocal的原理
    private Map<Thread, Long> map = new HashMap<>();

    @Override
    public Product productInfo(Long id) {
        return productMapper.selectById(id);
    }

    @Override
    public PageInfoVo productPageInfo(PmsProductQueryParam param) {

        QueryWrapper<Product> wrapper = new QueryWrapper<>();

        //品牌
        if (param.getBrandId() != null) {
            //前端传了
            wrapper.eq("brand_id", param.getBrandId());
        }
        //名称
        if (!StringUtils.isEmpty(param.getKeyword())) {
            wrapper.like("name", param.getKeyword());
        }
        //分类
        if (param.getProductCategoryId() != null) {
            wrapper.eq("product_category_id", param.getProductCategoryId());
        }
        //货号
        if (!StringUtils.isEmpty(param.getProductSn())) {
            wrapper.like("product_sn", param.getProductSn());
        }
        //发布状态
        if (param.getPublishStatus() != null) {
            wrapper.eq("publish_status", param.getPublishStatus());
        }
        //审核状态
        if (param.getVerifyStatus() != null) {
            wrapper.eq("verify_status", param.getVerifyStatus());
        }

        IPage<Product> page = productMapper.selectPage(new Page<Product>(param.getPageNum(), param.getPageSize()), wrapper);
        //总记录数      //总页码         //每页记录      / /每页数据              / /当前页
        PageInfoVo pageInfoVo = new PageInfoVo(page.getTotal(), page.getPages(), param.getPageSize(), page.getRecords(), page.getCurrent());


        return pageInfoVo;
    }


    //大保存 保存商品
    //@Transactional
    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void saveProduct(PmsProductParam productParam) {
        ProductServiceImpl proxy = (ProductServiceImpl) AopContext.currentProxy();

        //1）、pms_product：保存商品基本信息
        proxy.saveBaseInfo(productParam);

        //5）、pms_sku_stock：sku_库存表  //保存商品的库存
        proxy.saveSkuStock(productParam);

        //2）、pms_product_attribute_value：
        //保存商品对应的所有属性的值
        proxy.saveProductAttributeValue(productParam);

        //保存商品的满减信息
        //3）、pms_product_full_reduction：
        proxy.saveFullReduction(productParam);

        //保存商品的阶梯价格
        //4）、pms_product_ladder：
        proxy.saveProductLadder(productParam);


    }

    //上架下架
    @Override
    public void updatePublishStatus(List<Long> ids, Integer publishStatus) {
        if (publishStatus == 0) {
            ids.forEach((id) -> {
                //下架
                //改数据库状态删es
                setProductPublishStatus(publishStatus, id);

                deleteProductFromEs(id);

            });


        } else {
            //上架

            ids.forEach((id) -> {
                //对数据库修改状态添加es
                setProductPublishStatus(publishStatus, id);
                saveProductToEs(id);

                /**
                 * #查询出某个商品的基本属性以及对应的值
                 SELECT pav.*,pa.`name` FROM pms_product_attribute_value pav
                 LEFT JOIN pms_product_attribute pa
                 ON pa.`id`=pav.`product_attribute_id`
                 WHERE pav.`product_id`=23 AND pa.`type`=1
                 */

                //把商品所有的sku信息拿出来

            });
        }


    }

    private void deleteProductFromEs(Long id) {
        Delete build = new Delete.Builder(id.toString()).index(EsConstant.PRODUCT_ES_INDEX)
                .type(EsConstant.PRODUCT_INFO_ES_TYPE)
                .build();
        try {
            DocumentResult execute = jestClient.execute(build);
            if(execute.isSucceeded()){
                log.error("商品:{}ES下架成功",id);
            }else {
                log.error("商品:{}ES下架失败",id);
            }

        }catch (Exception e){
            log.error("商品:{}ES下架失败",id);
        }
    }

    public void saveProductToEs(Long id) {
        //1.查出商品的基本信息
        Product productInfo = productInfo(id);

        EsProduct esProduct = new EsProduct();
        //把商品基本信息拷到es里（香品详情）数据库里的
        BeanUtils.copyProperties(productInfo, esProduct);

        //sku信息。对ES保存商品信息,还要查出商品的SKU，给ES中保存】
        List<SkuStock> stocks = skuStockMapper.selectList(new QueryWrapper<SkuStock>().eq("product_id", id));
        List<EsSkuProductInfo> esSkuProductInfos = new ArrayList<>(stocks.size());

        /**
         *
         * #查询当前商品sku值
         SELECT * FROM pms_product_attribute pa
         WHERE pa.`product_attribute_category_id` =
         (
         SELECT pa.`product_attribute_category_id`FROM pms_product_attribute_value pav
         LEFT JOIN pms_product_attribute pa
         ON pa.`id`=pav.`product_attribute_id`
         WHERE pav.`product_id`={} AND pa.`type`=0 LIMIT 1
         ) AND pa.`type`=0 ORDER BY pa.`sort` DESC
         *
         */

        //查出当前商品sku属性
        List<ProductAttribute> skuAttributeName = productAttributeValueMapper.selectProductSaleAttrName();

        stocks.forEach((skuStock) -> {
            //遍历每个sku基本属性
            EsSkuProductInfo info = new EsSkuProductInfo();
            //放到sku集合中
            BeanUtils.copyProperties(skuStock, info);

            //sku标题
            String sbuTile = esProduct.getName();
            if (!StringUtils.isEmpty(skuStock.getSp1())) {
                sbuTile += " " + skuStock.getSp1();
            }
            if (!StringUtils.isEmpty(skuStock.getSp2())) {
                sbuTile += " " + skuStock.getSp2();
            }
            if (!StringUtils.isEmpty(skuStock.getSp3())) {
                sbuTile += " " + skuStock.getSp3();
            }
            //sku的特色标题
            info.setSkuTitle(sbuTile);

            List<EsProductAttributeValue> skuAttributeValues = new ArrayList<>();
            //sku中销售属性的值
            for (int i = 0; i < skuAttributeName.size(); i++) {
                //skuAttr  颜色 尺码
                EsProductAttributeValue value = new EsProductAttributeValue();
                value.setName(skuAttributeName.get(i).getName());
                value.setProductId(id);
                value.setProductAttributeId(skuAttributeName.get(i).getId());
                value.setType(skuAttributeName.get(i).getType());

                //颜色尺码 让es统计
                if (i == 0) {
                    value.setValue(skuStock.getSp1());
                }
                if (i == 1) {
                    value.setValue(skuStock.getSp2());
                }
                if (i == 2) {
                    value.setValue(skuStock.getSp3());
                }

                skuAttributeValues.add(value);
            }


            info.setAttributeValues(skuAttributeValues);

            esSkuProductInfos.add(info);
            //查出销售属性的名

        });

        esProduct.setSkuProductInfos(esSkuProductInfos);
        //spu直接去数据库查出来
        List<EsProductAttributeValue> attributeValues = productAttributeValueMapper.selectProductBaseAttrAndValue(id);
        //赋值公共属性信息
        esProduct.setAttrValueList(attributeValues);

        //把商品保存到es中
        try {

            Index build = new Index.Builder(esProduct)
                    .index(EsConstant.PRODUCT_ES_INDEX)
                    .type(EsConstant.PRODUCT_INFO_ES_TYPE)
                    .id(id.toString())
                    .build();
            DocumentResult execute = jestClient.execute(build);
            boolean succeeded = execute.isSucceeded();
            if(succeeded){
                log.info("ES中：id为{}商品上架成功",id);
            }else {
                log.error("ES中：id为{}商品未保存成功",id);
            }

        }catch (Exception e){
            log.error("ESs商品保存异常:{}",e.getMessage());
        }

    }

    public void setProductPublishStatus(Integer publishStatus, Long id) {
        Product product = new Product();
        product.setId(id);
        product.setPublishStatus(publishStatus);
        productMapper.updateById(product);
    }

    //新品
    @Override
    public void updateNewStatus(List<Long> ids, Integer newStatus) {
        List<Product> products = productMapper.selectBatchIds(ids);
        Product product = products.get(0);
        Product product1 = product.setNewStatus(newStatus);
        productMapper.updateById(product1);
    }

    //推荐
    @Override
    public void updateRecommendStatus(List<Long> ids, Integer recommendStatus) {

        List<Product> products = productMapper.selectBatchIds(ids);
        Product product = products.get(0);
        Product product1 = product.setRecommandStatus(recommendStatus);
        productMapper.updateById(product1);
    }

    //审核状态
    @Override
    public void updateVerifyStatus(List<Long> ids, Integer verifyStatus, String detail) {
        List<Product> products = productMapper.selectBatchIds(ids);
        Product product = products.get(0);
        Product product1 = product.setVerifyStatus(verifyStatus);
        productMapper.updateById(product1);
    }


    //删除状态
    @Override
    public void updateDeleteStatus(List<Long> ids, Integer deleteStatus) {
        List<Product> products = productMapper.selectBatchIds(ids);
        Product product = products.get(0);
        Product product1 = product.setDeleteStatus(deleteStatus);
        productMapper.updateById(product1);
    }

    @Override
    public EsProduct productAllInfo(Long id) {

        EsProduct esProduct = null;

        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.termQuery("id",id));

        Search build = new Search.Builder(builder.toString())
                .addIndex(EsConstant.PRODUCT_ES_INDEX)
                .addType(EsConstant.PRODUCT_INFO_ES_TYPE)
                .build();
        try {
            SearchResult execute = jestClient.execute(build);
            List<SearchResult.Hit<EsProduct, Void>> hits = execute.getHits(EsProduct.class);
            esProduct = hits.get(0).source;
        } catch (IOException e) {

        }

        return esProduct;
    }

    @Override
    public EsProduct produSkuInfo(Long id) {
        EsProduct esProduct = null;

        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.nestedQuery("skuProductInfos",QueryBuilders.termQuery("skuProductInfos.id",id), ScoreMode.None));

        Search build = new Search.Builder(builder.toString())
                .addIndex(EsConstant.PRODUCT_ES_INDEX)
                .addType(EsConstant.PRODUCT_INFO_ES_TYPE)
                .build();
        try {
            SearchResult execute = jestClient.execute(build);
            List<SearchResult.Hit<EsProduct, Void>> hits = execute.getHits(EsProduct.class);
            esProduct = hits.get(0).source;
        } catch (IOException e) {

        }

        return esProduct;
    }


    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void saveSkuStock(PmsProductParam productParam) {
        List<SkuStock> skuStockList = productParam.getSkuStockList();
        for (int i = 1; i <= skuStockList.size(); i++) {
            SkuStock skuStock = skuStockList.get(i - 1);
            if (StringUtils.isEmpty(skuStock.getSkuCode())) {
                //skuCode必须有  1_1  1_2 1_3 1_4
                //生成规则  商品id_sku自增id
                skuStock.setSkuCode(threadLocal.get() + "_" + i);
            }
            skuStock.setProductId(threadLocal.get());
            skuStockMapper.insert(skuStock);
        }
        log.debug("当前线程号:{}", Thread.currentThread().getId());
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void saveProductLadder(PmsProductParam productParam) {
        List<ProductLadder> productLadderList = productParam.getProductLadderList();
        productLadderList.forEach((productLadder) -> {
            productLadder.setProductId(threadLocal.get());
            productLadderMapper.insert(productLadder);

        });
        log.debug("当前线程号:{}", Thread.currentThread().getId());
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void saveFullReduction(PmsProductParam productParam) {
        List<ProductFullReduction> fullReductionList = productParam.getProductFullReductionList();
        fullReductionList.forEach((reduction) -> {
            reduction.setProductId(threadLocal.get());
            productFullReductionMapper.insert(reduction);
        });
        log.debug("当前线程号:{}", Thread.currentThread().getId());
        int i = 10 / 0;
    }

    /**
     * 保存商品基础信息
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void saveBaseInfo(PmsProductParam productParam) {
        //1）、pms_product：保存商品基本信息
        Product product = new Product();
        BeanUtils.copyProperties(productParam, product);
        productMapper.insert(product);

        threadLocal.set(product.getId());
        map.put(Thread.currentThread(), product.getId());
        log.debug("当前线程号:{}", Thread.currentThread().getId());


    }

    //2）、pms_product_attribute_value：保存这个商品对应的所有属性的值
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void saveProductAttributeValue(PmsProductParam productParam) {
        List<ProductAttributeValue> valueList = productParam.getProductAttributeValueList();
        valueList.forEach((item) -> {
            Long aLong = map.get(Thread.currentThread());
            System.out.println("利用map存储数据" + aLong);
            item.setProductId(threadLocal.get());
            productAttributeValueMapper.insert(item);
        });

        log.debug("当前线程号:{}", Thread.currentThread().getId());
    }
}