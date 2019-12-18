CREATE TABLE `dynamic_axletree_tem_limit` (
  `id` int(10) NOT NULL,
  `line_code` varchar(50) DEFAULT NULL COMMENT '线路编号',
  `direction_code` varchar(50) DEFAULT NULL COMMENT '行别编号',
  `mileage` varchar(50) DEFAULT NULL COMMENT '里程',
  `train_type` varchar(50) DEFAULT NULL COMMENT '列车类型',
  `train_id` varchar(50) DEFAULT NULL COMMENT '列号',
  `train_coach_no` varchar(50) DEFAULT NULL COMMENT '车厢号',
  `env_temperature` varchar(50) DEFAULT NULL COMMENT '环温',
  `axletree_type1` varchar(50) DEFAULT NULL COMMENT '轴承类型1',
  `axletree_tem_upper_limit1` varchar(50) DEFAULT NULL COMMENT '轴温上限1',
  `axletree_tem_lower_limit1` varchar(50) DEFAULT NULL COMMENT '轴温下限1',
  PRIMARY KEY (`id`)
) ENGINE=ndbcluster DEFAULT CHARSET=utf8;