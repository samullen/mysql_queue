CREATE TABLE process_queue (
  id int(11) NOT NULL AUTO_INCREMENT,
  namespace varchar(255) DEFAULT NULL,
  payload text NOT NULL,
  status enum('new','open','closed','error') NOT NULL DEFAULT 'new',
  created_on date NOT NULL,
  updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id)
) ENGINE=InnoDB
