CREATE TABLE `real_time_all_axletree_temperature`(
  `train_line` string,                                             -- 线路
  `train_line_code` string,                                        -- 线路编号
  `line_dire` string,                                              -- 行别
  `line_dire_code` string,                                         -- 行别编号
  `train_speed` string,                                            -- 列车速度
  `mileage` string,                                                -- 里程
  `rowkey_coach` string,                                           -- hbase不同车厢对应的rowkey
  `real_time` string,                                              -- 实时时间
  `train_type` string,                                             -- 列车型号
  `train_id` string,                                               -- 列车id
  `coach_no` string,                                               -- 车厢号
  `env_temperature_display_code` string,                           -- 新风温度display_code
  `env_temperature_data_code` string,                              -- 新风温度data_code
  `env_temperature_data_name` string,                              -- 新风温度data_name
  `env_temperature` string,                                        -- 新风温度

  `axletree_display_code1` string,                                 -- 1位轴箱轴承display_code1
  `axletree_data_code1` string,                                    -- 1位轴箱轴承data_code1
  `axletree_data_name1` string,                                    -- 1位轴箱轴承名1
  `axletree_temperature1` string,                                  -- 1位轴箱轴承温度1

  `axletree_display_code2` string,                                 -- 2位轴箱轴承display_code2
  `axletree_data_code2` string,                                    -- 2位轴箱轴承data_code2
  `axletree_data_name2` string,                                    -- 2位轴箱轴承名2
  `axletree_temperature2` string,                                  -- 2位轴箱轴承温度2

  `axletree_display_code3` string,                                 -- 3位轴箱轴承display_code3
  `axletree_data_code3` string,                                    -- 3位轴箱轴承data_code3
  `axletree_data_name3` string,                                    -- 3位轴箱轴承名3
  `axletree_temperature3` string,                                  -- 3位轴箱轴承温度3

  `axletree_display_code4` string,                                 -- 4位轴箱轴承display_code4
  `axletree_data_code4` string,                                    -- 4位轴箱轴承data_code4
  `axletree_data_name4` string,                                    -- 4位轴箱轴承名4
  `axletree_temperature4` string,                                  -- 4位轴箱轴承温度4

  `axletree_display_code5` string,                                 -- 5位轴箱轴承display_code5
  `axletree_data_code5` string,                                    -- 5位轴箱轴承data_code5
  `axletree_data_name5` string,                                    -- 5位轴箱轴承名5
  `axletree_temperature5` string,                                  -- 5位轴箱轴承温度5

  `axletree_display_code6` string,                                 -- 6位轴箱轴承display_code6
  `axletree_data_code6` string,                                    -- 6位轴箱轴承data_code6
  `axletree_data_name6` string,                                    -- 6位轴箱轴承名6
  `axletree_temperature6` string,                                  -- 6位轴箱轴承温度6

  `axletree_display_code7` string,                                 -- 7位轴箱轴承display_code7
  `axletree_data_code7` string,                                    -- 7位轴箱轴承data_code7
  `axletree_data_name7` string,                                    -- 7位轴箱轴承名7
  `axletree_temperature7` string,                                  -- 7位轴箱轴承温度7

  `axletree_display_code8` string,                                 -- 8位轴箱轴承display_code8
  `axletree_data_code8` string,                                    -- 8位轴箱轴承data_code8
  `axletree_data_name8` string,                                    -- 8位轴箱轴承名8
  `axletree_temperature8` string,                                  -- 8位轴箱轴承温度8

  `axletree_display_code9` string,                                 -- 1轴小齿轮箱电机侧轴承温度轴承display_code9
  `axletree_data_code9` string,                                    -- 1轴小齿轮箱电机侧轴承温度轴承data_code9
  `axletree_data_name9` string,                                    -- 1轴小齿轮箱电机侧轴承温度轴承名9
  `axletree_temperature9` string,                                  -- 1轴小齿轮箱电机侧轴承温度轴承温度9
   
  `axletree_display_code10` string,                                -- 2轴小齿轮箱电机侧轴承温度轴承display_code10
  `axletree_data_code10` string,                                   -- 2轴小齿轮箱电机侧轴承温度轴承data_code10
  `axletree_data_name10` string,                                   -- 2轴小齿轮箱电机侧轴承温度轴承名10
  `axletree_temperature10` string,                                 -- 2轴小齿轮箱电机侧轴承温度轴承温度10
   
  `axletree_display_code11` string,                                -- 3轴小齿轮箱电机侧轴承温度轴承display_code11
  `axletree_data_code11` string,                                   -- 3轴小齿轮箱电机侧轴承温度轴承data_code11
  `axletree_data_name11` string,                                   -- 3轴小齿轮箱电机侧轴承温度轴承名11
  `axletree_temperature11` string,                                 -- 3轴小齿轮箱电机侧轴承温度轴承温度11
   
  `axletree_display_code12` string,                                -- 4轴小齿轮箱电机侧轴承温度轴承display_code12
  `axletree_data_code12` string,                                   -- 4轴小齿轮箱电机侧轴承温度轴承data_code12
  `axletree_data_name12` string,                                   -- 4轴小齿轮箱电机侧轴承温度轴承名12
  `axletree_temperature12` string,                                 -- 4轴小齿轮箱电机侧轴承温度轴承温度12
   
  `axletree_display_code13` string,                                -- 1轴小齿轮箱车轮侧轴承温度轴承display_code13
  `axletree_data_code13` string,                                   -- 1轴小齿轮箱车轮侧轴承温度轴承data_code13
  `axletree_data_name13` string,                                   -- 1轴小齿轮箱车轮侧轴承温度轴承名13
  `axletree_temperature13` string,                                 -- 1轴小齿轮箱车轮侧轴承温度轴承温度13
   
  `axletree_display_code14` string,                                -- 2轴小齿轮箱车轮侧轴承温度轴承display_code14
  `axletree_data_code14` string,                                   -- 2轴小齿轮箱车轮侧轴承温度轴承data_code14
  `axletree_data_name14` string,                                   -- 2轴小齿轮箱车轮侧轴承温度轴承名14
  `axletree_temperature14` string,                                 -- 2轴小齿轮箱车轮侧轴承温度轴承温度14
   
  `axletree_display_code15` string,                                -- 3轴小齿轮箱车轮侧轴承温度轴承display_code15
  `axletree_data_code15` string,                                   -- 3轴小齿轮箱车轮侧轴承温度轴承data_code15
  `axletree_data_name15` string,                                   -- 3轴小齿轮箱车轮侧轴承温度轴承名15
  `axletree_temperature15` string,                                 -- 3轴小齿轮箱车轮侧轴承温度轴承温度15
  
  `axletree_display_code16` string,                                -- 4轴小齿轮箱车轮侧轴承温度轴承display_code16
  `axletree_data_code16` string,                                   -- 4轴小齿轮箱车轮侧轴承温度轴承data_code16
  `axletree_data_name16` string,                                   -- 4轴小齿轮箱车轮侧轴承温度轴承名16
  `axletree_temperature16` string,                                 -- 4轴小齿轮箱车轮侧轴承温度轴承温度16
   
  `axletree_display_code17` string,                                -- 1轴电机定子温度轴承display_code17
  `axletree_data_code17` string,                                   -- 1轴电机定子温度轴承data_code17
  `axletree_data_name17` string,                                   -- 1轴电机定子温度轴承名17
  `axletree_temperature17` string,                                 -- 1轴电机定子温度轴承温度17
   
  `axletree_display_code18` string,                                -- 1轴电机传动端轴承温度轴承display_code18
  `axletree_data_code18` string,                                   -- 1轴电机传动端轴承温度轴承data_code18
  `axletree_data_name18` string,                                   -- 1轴电机传动端轴承温度轴承名18
  `axletree_temperature18` string,                                 -- 1轴电机传动端轴承温度轴承温度18
   
  `axletree_display_code19` string,                                -- 1轴电机非传动端轴承温度轴承display_code19
  `axletree_data_code19` string,                                   -- 1轴电机非传动端轴承温度轴承data_code19
  `axletree_data_name19` string,                                   -- 1轴电机非传动端轴承温度轴承名19
  `axletree_temperature19` string,                                 -- 1轴电机非传动端轴承温度轴承温度19
    
  `axletree_display_code20` string,                                -- 2轴电机定子温度轴承display_code20
  `axletree_data_code20` string,                                   -- 2轴电机定子温度轴承data_code20
  `axletree_data_name20` string,                                   -- 2轴电机定子温度轴承名20
  `axletree_temperature20` string,                                 -- 2轴电机定子温度轴承温度20
   
  `axletree_display_code21` string,                                -- 2轴电机传动端轴承温度轴承display_code21
  `axletree_data_code21` string,                                   -- 2轴电机传动端轴承温度轴承data_code21
  `axletree_data_name21` string,                                   -- 2轴电机传动端轴承温度轴承名21
  `axletree_temperature21` string,                                 -- 2轴电机传动端轴承温度轴承温度21
   
  `axletree_display_code22` string,                                -- 2轴电机非传动端轴承温度轴承display_code22
  `axletree_data_code22` string,                                   -- 2轴电机非传动端轴承温度轴承data_code22
  `axletree_data_name22` string,                                   -- 2轴电机非传动端轴承温度轴承名22
  `axletree_temperature22` string,                                 -- 2轴电机非传动端轴承温度轴承温度22
   
  `axletree_display_code23` string,                                -- 3轴电机定子温度轴承display_code23
  `axletree_data_code23` string,                                   -- 3轴电机定子温度轴承data_code23
  `axletree_data_name23` string,                                   -- 3轴电机定子温度轴承名23
  `axletree_temperature23` string,                                 -- 3轴电机定子温度轴承温度23
   
  `axletree_display_code24` string,                                -- 3轴电机传动端轴承温度轴承display_code24
  `axletree_data_code24` string,                                   -- 3轴电机传动端轴承温度轴承data_code24
  `axletree_data_name24` string,                                   -- 3轴电机传动端轴承温度轴承名24
  `axletree_temperature24` string,                                 -- 3轴电机传动端轴承温度轴承温度24
    
  `axletree_display_code25` string,                                -- 3轴电机非传动端轴承温度轴承display_code25
  `axletree_data_code25` string,                                   -- 3轴电机非传动端轴承温度轴承data_code25
  `axletree_data_name25` string,                                   -- 3轴电机非传动端轴承温度轴承名25
  `axletree_temperature25` string,                                 -- 3轴电机非传动端轴承温度轴承温度25
    
  `axletree_display_code26` string,                                -- 4轴电机定子温度轴承display_code26
  `axletree_data_code26` string,                                   -- 4轴电机定子温度轴承data_code26
  `axletree_data_name26` string,                                   -- 4轴电机定子温度轴承名26
  `axletree_temperature26` string,                                 -- 4轴电机定子温度轴承温度26
   
  `axletree_display_code27` string,                                -- 4轴电机传动端轴承温度轴承display_code27
  `axletree_data_code27` string,                                   -- 4轴电机传动端轴承温度轴承data_code27
  `axletree_data_name27` string,                                   -- 4轴电机传动端轴承温度轴承名27
  `axletree_temperature27` string,                                 -- 4轴电机传动端轴承温度轴承温度27
   
  `axletree_display_code28` string,                                -- 4轴电机非传动端轴承温度轴承display_code28
  `axletree_data_code28` string,                                   -- 4轴电机非传动端轴承温度轴承data_code28
  `axletree_data_name28` string,                                   -- 4轴电机非传动端轴承温度轴承名28
  `axletree_temperature28` string,                                 -- 4轴电机非传动端轴承温度轴承温度28
   
  `axletree_display_code29` string,                                -- 1轴大齿轮箱电机侧轴承温度轴承display_code29
  `axletree_data_code29` string,                                   -- 1轴大齿轮箱电机侧轴承温度轴承data_code29
  `axletree_data_name29` string,                                   -- 1轴大齿轮箱电机侧轴承温度轴承名29
  `axletree_temperature29` string,                                 -- 1轴大齿轮箱电机侧轴承温度轴承温度29
   
  `axletree_display_code30` string,                                -- 2轴大齿轮箱电机侧轴承温度轴承display_code30
  `axletree_data_code30` string,                                   -- 2轴大齿轮箱电机侧轴承温度轴承data_code30
  `axletree_data_name30` string,                                   -- 2轴大齿轮箱电机侧轴承温度轴承名30
  `axletree_temperature30` string,                                 -- 2轴大齿轮箱电机侧轴承温度轴承温度30
   
  `axletree_display_code31` string,                                -- 3轴大齿轮箱电机侧轴承温度轴承display_code31
  `axletree_data_code31` string,                                   -- 3轴大齿轮箱电机侧轴承温度轴承data_code31
  `axletree_data_name31` string,                                   -- 3轴大齿轮箱电机侧轴承温度轴承名31
  `axletree_temperature31` string,                                 -- 3轴大齿轮箱电机侧轴承温度轴承温度31
   
  `axletree_display_code32` string,                                -- 4轴大齿轮箱电机侧轴承温度轴承display_code32
  `axletree_data_code32` string,                                   -- 4轴大齿轮箱电机侧轴承温度轴承data_code32
  `axletree_data_name32` string,                                   -- 4轴大齿轮箱电机侧轴承温度轴承名32
  `axletree_temperature32` string,                                 -- 4轴大齿轮箱电机侧轴承温度轴承温度32
   
  `axletree_display_code33` string,                                -- 1轴大齿轮箱车轮侧轴承温度轴承display_code33
  `axletree_data_code33` string,                                   -- 1轴大齿轮箱车轮侧轴承温度轴承data_code33
  `axletree_data_name33` string,                                   -- 1轴大齿轮箱车轮侧轴承温度轴承名33
  `axletree_temperature33` string,                                 -- 1轴大齿轮箱车轮侧轴承温度轴承温度33
   
  `axletree_display_code34` string,                                -- 2轴大齿轮箱车轮侧轴承温度轴承display_code34
  `axletree_data_code34` string,                                   -- 2轴大齿轮箱车轮侧轴承温度轴承data_code34
  `axletree_data_name34` string,                                   -- 2轴大齿轮箱车轮侧轴承温度轴承名34
  `axletree_temperature34` string,                                 -- 2轴大齿轮箱车轮侧轴承温度轴承温度34
   
  `axletree_display_code35` string,                                -- 3轴大齿轮箱车轮侧轴承温度轴承display_code35
  `axletree_data_code35` string,                                   -- 3轴大齿轮箱车轮侧轴承温度轴承data_code35
  `axletree_data_name35` string,                                   -- 3轴大齿轮箱车轮侧轴承温度轴承名35
  `axletree_temperature35` string,                                 -- 3轴大齿轮箱车轮侧轴承温度轴承温度35
    
  `axletree_display_code36` string,                                -- 4轴大齿轮箱车轮侧轴承温度轴承display_code36
  `axletree_data_code36` string,                                   -- 4轴大齿轮箱车轮侧轴承温度轴承data_code36
  `axletree_data_name36` string,                                   -- 4轴大齿轮箱车轮侧轴承温度轴承名36
  `axletree_temperature36` string,                                 -- 4轴大齿轮箱车轮侧轴承温度轴承温度36

  `time` string                                                    -- 更新时间
  )
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
