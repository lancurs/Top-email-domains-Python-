CREATE TABLE `domain` (
  `ID` bigint(20) NOT NULL AUTO_INCREMENT,
  `domain` varchar(15) NOT NULL,
  `count` int(11) DEFAULT NULL,
  `date` date DEFAULT NULL,
  PRIMARY KEY (`ID`)
)