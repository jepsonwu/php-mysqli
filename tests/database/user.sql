/*
Navicat MySQL Data Transfer

Source Server         : in_dev
Source Server Version : 50627
Source Host           : 10.10.106.218:3306
Source Database       : inchat_user

Target Server Type    : MYSQL
Target Server Version : 50627
File Encoding         : 65001

Date: 2017-02-14 17:21:40
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for user
-- ----------------------------
DROP TABLE IF EXISTS `user`;
CREATE TABLE `user` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `token` char(32) NOT NULL DEFAULT '' COMMENT '用户登录标识',
  `gender` char(3) NOT NULL DEFAULT '' COMMENT '性别',
  `birthday` char(10) NOT NULL DEFAULT '',
  `school` char(20) NOT NULL DEFAULT '' COMMENT '学校',
  `city` char(20) NOT NULL DEFAULT '' COMMENT '城市',
  `gps` char(20) NOT NULL DEFAULT '',
  `ip` char(25) NOT NULL DEFAULT '',
  `personal_tag` text NOT NULL COMMENT '兴趣标签',
  `created_at` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '创建时间',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=100007873 DEFAULT CHARSET=utf8;
