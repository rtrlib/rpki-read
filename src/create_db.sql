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
