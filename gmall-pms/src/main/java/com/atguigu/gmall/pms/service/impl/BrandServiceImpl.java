package com.atguigu.gmall.pms.service.impl;

import com.alibaba.dubbo.config.annotation.Service;
import com.atguigu.gmall.pms.entity.Brand;
import com.atguigu.gmall.pms.mapper.BrandMapper;
import com.atguigu.gmall.pms.service.BrandService;
import com.atguigu.gmall.vo.PageInfoVo;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * <p>
 * 品牌表 服务实现类
 * </p>
 *
 * @author zs
 * @since 2019-05-09
 */
@Service
@Component
@Slf4j
public class BrandServiceImpl extends ServiceImpl<BrandMapper, Brand> implements BrandService {

    @Autowired
    BrandMapper brandMapper;

    //品牌分页查询
    @Override
    public PageInfoVo beandPageInfo(String keyword, Integer pageNum, Integer pageSize) {

        QueryWrapper<Brand> name = null;

        if(!StringUtils.isEmpty(keyword)){
            name = new QueryWrapper<Brand>().like("name", keyword);
        }

        IPage<Brand> brandIPage = brandMapper.selectPage(new Page<Brand>(pageNum.longValue(), pageSize.longValue()), name);

        PageInfoVo pageInfoVo = new PageInfoVo(brandIPage.getTotal(), brandIPage.getPages(), pageSize.longValue(), brandIPage.getRecords(), brandIPage.getCurrent());

        return pageInfoVo;
    }
}
