CREATE TABLE geoip2_location (
  geoname_id integer not null,
  locale_code text not null,
  continent_code text,
  continent_name text,
  country_iso_code text,
  country_name text,
  subdivision_1_iso_code text default null,
  subdivision_1_name text default null,
  subdivision_2_iso_code text default null,
  subdivision_2_name text default null,
  city_name text default null,
  metro_code integer default null,
  time_zone text default null,
  is_in_european_union boolean,
  last_modified date not null default current_date,
  primary key (geoname_id, locale_code, last_modified)
);

CREATE VIEW geoip2_location_country AS
SELECT
  geoname_id,
  locale_code,
  continent_code,
  continent_name,
  country_iso_code,
  country_name,
  is_in_european_union,
  last_modified
FROM geoip2_location;

CREATE TABLE geoip2_network (
  network cidr not null,
  geoname_id integer,
  registered_country_geoname_id integer,
  represented_country_geoname_id integer,
  is_anonymous_proxy boolean,
  is_satellite_provider boolean,
  postal_code text default null,
  latitude numeric default null,
  longitude numeric default null,
  accuracy_radius integer default null,
  is_anycast boolean,
  last_modified date not null default current_date,
  primary key (network, last_modified)
);

CREATE VIEW geoip2_network_country AS
SELECT
  network,
  geoname_id,
  registered_country_geoname_id,
  represented_country_geoname_id,
  is_anonymous_proxy,
  is_satellite_provider,
  is_anycast,
  last_modified
FROM geoip2_network;

CREATE TABLE geoip2_asn (
  network cidr not null,
  autonomous_system_number integer,
  autonomous_system_organization text,
  last_modified date not null default current_date,
  primary key (network, last_modified)
);
