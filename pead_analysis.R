library(DBI)
library(dplyr, warn.conflicts = FALSE)
library(tidyr)
Sys.setenv(PGHOST = "10.101.13.99", PGDATABASE="crsp")
Sys.setenv(PGUSER="siyuanh1", PGPASSWORD="temp_20190510")
pg <- dbConnect(RPostgres::Postgres())
funda <- tbl(pg, sql("SELECT * FROM comp.funda"))
fundq <- tbl(pg, sql("SELECT * FROM comp.fundq"))
trading_days <- tbl(pg, sql("SELECT * FROM crsp.dsi"))

gb_news<-
  funda%>%
  filter(fyear > 1976) %>%
  filter(is.na(opeps)== FALSE) %>%
  select(cusip, fyear, opeps) %>%
  arrange(fyear) %>%
  group_by(cusip) %>%
  mutate(news = ifelse(opeps>=lag(opeps), "good", "bad")) %>%
  filter(is.na(news) == FALSE) %>%
  ungroup() %>%
  mutate(fyearq = as.integer(fyear), cus = substr(cusip,1,8)) %>%
  select(cus, fyearq, news) %>%
  rename(fyear = fyearq, cusip = cus)
#type of news for every fyear

annc_date<-
  fundq%>%
  select(cusip, fyearq, fqtr, rdq) %>%
  filter(fyearq>1976, fqtr == 4, is.na(rdq) == FALSE) %>%
  select(cusip, fyearq, rdq)
#anouncement date

eff_calend<-
  trading_days%>%
  select(date) %>%
  filter(date > as.Date("1976-01-01")) %>%
  mutate(td = rank(date))
date1<-summarise(eff_calend, min(date)) %>%pull()
date2<-summarise(eff_calend, max(date)) %>%pull()
calendar<-
  tibble(date = seq(from = date1, to = date2, by = 1))


eff_dates<- calendar%>%
  left_join(eff_calend,'copy' = TRUE) %>%
  arrange(date) %>%
  fill(td, .direction = "up") %>%
  rename(eff_date = date) %>%
  left_join(eff_calend, 'copy' = TRUE) %>%
  rename(annc_date = eff_date, eff_date = date) %>%
  select(annc_date, eff_date)
#effective dates of annoucements


daily_prc <- tbl(pg, sql("SELECT * FROM crsp.dsf"))
dsi <- tbl(pg, sql("SELECT * FROM crsp.dsi"))
daily_ret<-
  daily_prc%>%
  select(cusip, date, ret) %>%
  left_join(dsi%>%select(date, vwretd)) %>%
  mutate(ab_ret = ret - vwretd) %>%
  select(cusip, date, ab_ret) %>%
  filter(date>as.Date("1976-01-01"))
daily_ret
#daily return of all firms


effect_dates<-
  annc_date%>%
  rename(annc_date = rdq) %>%
  left_join(eff_dates,'copy'= TRUE) %>%
  select(cusip, fyearq, eff_date) %>%
  rename(fyear = fyearq, anncdate = eff_date)
#effective annoucement dates of firm-year
effect_dates2<-
  effect_dates%>%
  select(cusip, anncdate, fyear) %>%
  mutate(lagfyear = as.integer(fyear + 1), leadfyear = as.integer(fyear +2))

temp1<-
  effect_dates2%>%
  mutate(fyearq = as.integer(fyear)) %>%
  select(cusip, fyearq, anncdate) %>%
  rename(fyear = fyearq)

temp2<-
  effect_dates2%>%
  select(cusip, lagfyear, anncdate) %>%
  rename(fyear = lagfyear)
temp3<-
  effect_dates2%>%
  select(cusip, leadfyear, anncdate) %>%
  rename(fyear = leadfyear)

temp_sum<-
  bind_rows(as.data.frame(temp1), as.data.frame(temp2), as.data.frame(temp3))
temp_final<-
  as_tibble(temp_sum) %>%
  arrange(cusip, fyear) %>%
  mutate(cus = substr(cusip,1,8)) %>%
  select(cus, fyear, anncdate) %>%
  rename(cusip = cus) %>%
  left_join(gb_news,'copy' =TRUE) %>%
  filter(is.na(news) == FALSE)
#dates to consider for every firm-year

end<-
  daily_ret%>%
  mutate(fyear = as.integer(substr(as.character(date),1,4))) %>%
  left_join(temp_final,'copy' = TRUE) %>%
  filter(is.na(anncdate) == FALSE) %>%
  mutate(day = date - anncdate) %>%
  filter(day<180, day>-360)

result<-
  end%>%
  select(ab_ret, day, news) %>%
  group_by(news, day) %>%
  summarise(avg_ret = mean(ab_ret)) %>%
  group_by(news) %>%
  arrange(day)

result



more<-
  result%>%
  as.data.frame(result)
write.csv(more, "filename.csv")
new_data<- read.csv("pead.data.csv")
library(ggplot2)
ggplot(new_data, mapping = aes(x = day, y = car, color = news))+
  geom_line()
