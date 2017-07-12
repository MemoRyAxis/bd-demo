package com.memoryaxis.hbase.controller;

import com.memoryaxis.hbase.dao.UserDao;
import com.memoryaxis.hbase.po.User;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author memoryaxis@gmail.com
 */
@RestController
@RequestMapping("/user")
@Api(value = "用户管理", tags = "user")
public class UserController {

    private static final Logger log = LoggerFactory.getLogger(UserController.class);

    @Autowired
    private UserDao userDao;

    @ApiOperation(value = "添加用户")
    @RequestMapping(value = "/user", method = RequestMethod.POST)
    public String insert(User user) {
        try {
            userDao.insert(user);
        } catch (Exception e) {
            log.error("Insert Fail!", e);
        }
        return null;
    }

    @ApiOperation(value = "更新用户")
    @RequestMapping(value = "/user", method = RequestMethod.PUT)
    public String update(User user) {
        try {
            userDao.update(user);
        } catch (Exception e) {
            log.error("Update Fail!", e);
        }
        return null;
    }

    @ApiOperation(value = "删除用户")
    @RequestMapping(value = "/user", method = RequestMethod.DELETE)
    public String delete(User user) {
        try {
            userDao.delete(user);
        } catch (Exception e) {
            log.error("Delete Fail!", e);
        }
        return null;
    }

    @ApiOperation(value = "获取用户")
    @RequestMapping(value = "/user", method = RequestMethod.GET)
    public String get(User user) {
        try {
            User r = userDao.get(user);
            return r.toString();
        } catch (Exception e) {
            log.error("Get Info Fail!", e);
        }
        return null;
    }
}
