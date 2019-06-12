library(DBI)
library(dplyr, warn.conflicts = FALSE)
library(tidyr)

Sys.setenv(PGHOST = "10.101.13.99", PGDATABASE="crsp")
Sys.setenv(PGUSER="siyuanh1", PGPASSWORD="temp_20190510")

pg <- dbConnect(RPostgres::Postgres())
rs <- dbExecute(pg, "SET work_mem = '3GB'")
funda <- tbl(pg, sql("SELECT * FROM comp.funda"))
fundq <- tbl(pg, sql("SELECT * FROM comp.fundq"))
trading_days <- tbl(pg, sql("SELECT * FROM crsp.dsi"))

# Type of news for every fyear
gb_news <-
  funda %>%
  filter(fyear > 1976) %>%
  filter(!is.na(opeps)) %>%
  select(cusip, fyear, opeps) %>%
  arrange(fyear) %>%
  group_by(cusip) %>%
  mutate(news = if_else(opeps >= lag(opeps), "good", "bad")) %>%
  filter(!is.na(news)) %>%
  ungroup() %>%
  mutate(fyear = as.integer(fyear),
         cusip = substr(cusip, 1, 8)) %>%
  select(cusip, fyear, news)

# Announcement dates
annc_date <-
  fundq %>%
  select(cusip, fyearq, fqtr, rdq) %>%
  filter(fyearq > 1976, fqtr == 4, !is.na(rdq)) %>%
  select(cusip, fyearq, rdq)

eff_calend <-
  trading_days %>%
  select(date) %>%
  filter(date > "1976-01-01") %>%
  mutate(td = rank(date)) %>%
  collect()

calendar <-
  tibble(date = seq(from = summarise(eff_calend, min(date)) %>% pull(),
                    to = summarise(eff_calend, max(date)) %>% pull(),
                    by = 1))

# Effective dates of annoucements
eff_dates <-
  calendar %>%
  left_join(eff_calend) %>%
  arrange(date) %>%
  fill(td, .direction = "up") %>%
  rename(eff_date = date) %>%
  left_join(eff_calend) %>%
  rename(annc_date = eff_date, eff_date = date) %>%
  select(annc_date, eff_date) %>%
  copy_to(pg, ., overwrite = TRUE)

daily_prc <- tbl(pg, sql("SELECT * FROM crsp.dsf"))
dsi <- tbl(pg, sql("SELECT * FROM crsp.dsi"))

# Daily returns of all firms
daily_ret <-
  daily_prc %>%
  select(cusip, date, ret) %>%
  left_join(dsi %>% select(date, vwretd)) %>%
  mutate(ab_ret = ret - vwretd) %>%
  select(cusip, date, ab_ret) %>%
  filter(date > "1976-01-01")

daily_ret

# Effective announcement dates of firm-year
effect_dates <-
  annc_date %>%
  rename(annc_date = rdq) %>%
  left_join(eff_dates) %>%
  select(cusip, fyearq, eff_date) %>%
  rename(fyear = fyearq, anncdate = eff_date)

effect_dates2 <-
  effect_dates %>%
  select(cusip, anncdate, fyear) %>%
  mutate(lagfyear = as.integer(fyear + 1),
         leadfyear = as.integer(fyear +2))

temp1 <-
  effect_dates2 %>%
  mutate(fyearq = as.integer(fyear)) %>%
  select(cusip, fyearq, anncdate) %>%
  rename(fyear = fyearq)

temp2 <-
  effect_dates2 %>%
  select(cusip, lagfyear, anncdate) %>%
  rename(fyear = lagfyear)

temp3 <-
  effect_dates2 %>%
  select(cusip, leadfyear, anncdate) %>%
  rename(fyear = leadfyear)

temp_sum <-
  union_all(temp1, temp2, temp3)

# Dates to consider for every firm-year
temp_final <-
  temp_sum %>%
  arrange(cusip, fyear) %>%
  mutate(cusip = substr(cusip, 1, 8)) %>%
  select(cusip, fyear, anncdate) %>%
  left_join(gb_news) %>%
  filter(!is.na(news)) %>%
  compute()

end <-
  daily_ret %>%
  mutate(fyear = as.integer(date_part('year', date))) %>%
  left_join(temp_final) %>%
  filter(!is.na(anncdate)) %>%
  mutate(day = date - anncdate) %>%
  filter(day < 180, day > -360) %>%
  compute()

result <-
  end %>%
  select(ab_ret, day, news) %>%
  group_by(news, day) %>%
  summarise(avg_ret = mean(ab_ret)) %>%
  ungroup() %>%
  compute()

new_data <-
  result %>%
  group_by(news) %>%
  arrange(day) %>%
  mutate(car = cumsum(avg_ret)) %>%
  arrange(news, day) %>%
  collect()

library(ggplot2)
ggplot(new_data, mapping = aes(x = day, y = car, color = news)) +
  geom_line()
