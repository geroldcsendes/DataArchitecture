## This code scrapes population and GDP data of Tanzania to facilitate feature engineering

library(xml2)
library(rvest)
library(tidyverse)

# Scrape Wikipedia GDP data
url_wiki <- "https://en.wikipedia.org/wiki/List_of_regions_of_Tanzania_by_GDP"


gdp_percap <-  url_wiki %>%
  read_html() %>%
  html_node(xpath = '//*[@id="mw-content-text"]/div/table[2]') %>%
  html_table(fill = TRUE)

gdp_percap <- gdp_percap %>% 
  select(region = Region, gdp_percap = `GDP per capita in USD (PPP)`)
# Remove 'Region' from region names
gdp_percap$region <- gsub('Region','',gdp_percap$region)

# Convert char to numeric
gdp_percap$gdp_percap <- as.numeric(gsub(",", "", gdp_percap$gdp_percap))
gdp_percap <- arrange(gdp_percap, region)

# Save
write.csv(gdp_percap, './data-repo/gdp.csv')


# Scrape populations and regional data
url_city <- "https://www.citypopulation.de/en/tanzania/cities/"

regions <- url_city %>% 
  read_html() %>% 
  html_node(xpath = '//*[@id="tl"]') %>% 
  html_table(fill = T)
View(regions)
str(regions)

# Clean regions df
region_colnames <- colnames(regions)

# Drop blank cols
region_colnames <- region_colnames[region_colnames != ""]  

# Get rid of cols with missing colname
regions <- subset(regions, select=region_colnames)

# Filter columns and rename some of them
regions <- regions %>% 
  select(region = Name, capital = Capital, pop_2012 = `PopulationCensus (C)2012-08-26`, 
         pop_2018_proj = `PopulationProjection (P)2018-07-01`)

# Convert char to numeric
regions$pop_2012 <- as.numeric(gsub(",", "", regions$pop_2012))
regions$pop_2018_proj <- as.numeric(gsub(",", "", regions$pop_2018_proj))

# Save
write.csv(regions, './data-repo/regions.csv')


# Scrape major cities
major_cities <- url_city %>% 
  read_html() %>% 
  html_node(xpath = '//*[@id="tlc"]') %>%
  html_table(fill=T)
major_cities <- select(major_cities, city = Name, pop_2012 = `PopulationCensus (C)2012-08-26`)

# Convert char to numeric
major_cities$pop_2012 <- as.numeric(gsub(",", "", major_cities$pop_2012))

# Save
write.csv(major_cities, './data-repo/major_cities.csv')


# Scrape cities and urbanities
cities <- url_city %>% 
  read_html() %>% 
  html_node(xpath = '//*[@id="ts"]') %>% 
  html_table(fill = T)

# Filter and rename cols
cities <- cities %>% 
  select(city = Name, pop_2002 = `PopulationCensus (C)2002-08-01`, pop_2012 = `PopulationCensus (C)2012-08-26`)

# Convert char to numeric
cities$pop_2002 <- as.numeric(gsub(",", "", cities$pop_2002))
cities$pop_2012 <- as.numeric(gsub(",", "", cities$pop_2012))

# Save
write.csv(cities, 'data-repo/cities.csv')


### Select only gdp and regions and join them
gdp_percap <- read.csv('./data-repo/cleaned/gdp.csv', stringsAsFactors = FALSE)
regions <- read.csv('./data-repo/cleaned/regions.csv', stringsAsFactors = FALSE)

gdp_percap <- gdp_percap %>% 
  select(-X) %>% 
  arrange(region)

regions <- regions %>% 
  select(-X) %>% 
  arrange(region)

# Recode values for join
regions$region[regions$region == 'Kagera [West Lake] '] <- 'Kagera'
regions$region[regions$region == 'Pwani [Coast]'] <- 'Pwani'

# Remove trailing spaces from gdp
gdp_percap$region <- str_trim(gdp_percap$region)


View(gdp_percap)
View(regions)

economic <- left_join(gdp_percap, regions,  by = 'region')
write.csv(economic, './data-repo/cleaned/economic.csv', row.names = FALSE)
