-- -----------------------
-- table validity
-- -----------------------
CREATE TABLE IF NOT EXISTS t_validity (
  prefix        INET NOT NULL,
  origin        INT NOT NULL,
  state         TEXT NOT NULL,
  ts            timestamp without time zone NOT NULL,
  roas          JSON,
  next_hop      INET,
  src_asn       INT,
  src_addr      INET
);

-- -----------------------
-- table archive
-- -----------------------
CREATE TABLE IF NOT EXISTS t_archive (
  prefix        INET NOT NULL,
  origin        INT NOT NULL,
  state         TEXT NOT NULL,
  ts            timestamp without time zone NOT NULL,
  roas          JSON,
  next_hop      INET,
  src_asn       INT,
  src_addr      INET
);

-- -----------------------
-- table stats
-- -----------------------
CREATE TABLE IF NOT EXISTS t_stats (
  ts                timestamp without time zone NOT NULL,
  num_valid         INT,
  num_invalid_as    INT,
  num_invalid_len   INT,
  num_notfound      INT
);
