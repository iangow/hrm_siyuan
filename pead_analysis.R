library(DBI)
library(dplyr, warn.conflicts = FALSE)
library(tidyr)

Sys.setenv(PGHOST = "10.101.13.99", PGDATABASE="crsp")
Sys.setenv(PGUSER="siyuanh1", PGPASSWORD="temp_20190510")

pg <- dbConnect(RPostgres::Postgres())
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
  filter(date > as.Date("1976-01-01")) %>%
  mutate(td = rank(date))

date1 <- summarise(eff_calend, min(date)) %>% pull()
date2 <- summarise(eff_calend, max(date)) %>% pull()
calendar <-
  tibble(date = seq(from = date1, to = date2, by = 1))

# Effective dates of annoucements
eff_dates <-
  calendar %>%
  left_join(eff_calend, copy = TRUE) %>%
  arrange(date) %>%
  fill(td, .direction = "up") %>%
  rename(eff_date = date) %>%
  left_join(eff_calend, copy = TRUE) %>%
  rename(annc_date = eff_date, eff_date = date) %>%
  select(annc_date, eff_date)

daily_prc <- tbl(pg, sql("SELECT * FROM crsp.dsf"))
dsi <- tbl(pg, sql("SELECT * FROM crsp.dsi"))

# Daily returns of all firms
daily_ret <-
  daily_prc %>%
  select(cusip, date, ret) %>%
  left_join(dsi %>% select(date, vwretd)) %>%
  mutate(ab_ret = ret - vwretd) %>%
  select(cusip, date, ab_ret) %>%
  filter(date > as.Date("1976-01-01"))
daily_ret


# Effective annoucement dates of firm-year
effect_dates<-
  annc_date %>%
  rename(annc_date = rdq) %>%
  left_join(eff_dates, copy= TRUE) %>%
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

temp_sum<-
  bind_rows(as.data.frame(temp1), as.data.frame(temp2), as.data.frame(temp3))

# Dates to consider for every firm-year
temp_final <-
  as_tibble(temp_sum) %>%
  arrange(cusip, fyear) %>%
  mutate(cusip = substr(cusip, 1, 8)) %>%
  select(cusip, fyear, anncdate) %>%
  left_join(gb_news, copy =TRUE) %>%
  filter(!is.na(news))

end <-
  daily_ret %>%
  mutate(fyear = as.integer(substr(as.character(date), 1, 4))) %>%
  left_join(temp_final, copy = TRUE) %>%
  filter(!is.na(anncdate)) %>%
  mutate(day = date - anncdate) %>%
  filter(day < 180, day > -360)

result <-
  end %>%
  select(ab_ret, day, news) %>%
  group_by(news, day) %>%
  summarise(avg_ret = mean(ab_ret)) %>%
  group_by(news) %>%
  arrange(day)

result

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
