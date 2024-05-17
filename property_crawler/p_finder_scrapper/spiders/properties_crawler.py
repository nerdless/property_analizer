""" Crawler to fetch property data from the website lamudi.com.mx/yucatan/casa/for-sale/ 

    Fetching a list, for each listed property in lamudi-yucatan. Properties' data: geo-point, 
    property name, town, description, price, area in square meters, buil square meters, and
    bed rooms
    The adds are showed in batchs of 30. 
    Store the info in an individual file and upload it into 'property-finder-data' s3-bucket

"""
import scrapy
import json
import hjson
from .settings import MEXICAN_STATES, PROPERTY_KIND
from ..items import Property
import time

TITLE_XPATH = "//meta[@property='og:title']/@content"
JSON_XPATH = "//script[@type='application/ld+json']/text()"
PRICE_XPATH = "//div[@class='prices-and-fees__price']/text() | //span[@class='price-info__value']/text()"
COMMUNITY_FEES_XPATH = "//div[@class='prices-and-fees__community-fees__community-price']/text()"
PLOT_AREA_XPATH1 = "//span[@data-test='plot-area-value']/text()"
PLOT_AREA_XPATH2 = "//span[@data-test='floor-value']/text()"
PUBLICATION_DATE_XPATH = "//div[@class='left-details']/div[@class='date']/text()"
IMAGES_XPATH = "//div[@class='photos']//div[@class='swiper-slide']/img/@src | //div[@class='swiper-slide']/img/@src"
PUBLISHER_NAME_XPATH = "//div[@class='agency']//span[@data-test='agency-name']/text()"
PUBLISHER_PHONE_XPATH = "//div[@class='agency__info']/a[@class='agency__phone']/text()"
PUBLISHER_URL = "//div[@class='form-header']//span[@data-test='agency-name']/following-sibling::a/@href"
CONDITION_XPATH = "//div[@class='place-features']//div[@class='condition']/span[2]"
CONSTRUCTION_YEAR_XPATH = "//div[@class='place-features']//div[@class='year']/span[2]"
SECONDARY_JSON = "//script[contains(text(),'coordinates')]//text()"
SECONDARY_DESCRIPTION = "//div[@id='description-text']/@data-expandeddescription"
FACILITIES = "//div[@class='facilities']//text()"
NEXT_PAGE_XPATH = "//div[@class='pagination__box']/a[@data-test='pagination-next']/@href"


class PropertiesSpider(scrapy.Spider):  
    """ name is used as a reference of this code (spider) for scrapy commands
            such as 'scrapy crawl "name" '== instruction to run the spider
        allowed_domains is a safety feature that restrict the spider to crawling the given domain
            it allows to avoid accidental errors
        start_urls is the starting point 
        """
    name = 'properties'
    allowed_domains = ['www.lamudi.com.mx']
    #creo que tengo que cambiar los siguientes parametros
    custom_settings= {
        'AUTOTHROTTLE_START_DELAY': 3,
        'AUTOTHROTTLE_MAX_DELAY': 15.0,
        'CONCURRENT_REQUEST_PER_DOMAIN': 1,
        'DOWNLOAD_DELAY': 3,
        'CONCURRENT_REQUEST_PER_IP':1,
        'ROBOTSTXT_OBEY': False
        }

    def start_requests(self):
        for state in ["yucatan"]:
            for kind in PROPERTY_KIND:
                meta = {"state": state, "kind": kind}
                # yield scrapy.Request(f"https://www.lamudi.com.mx/{state}/{kind}/for-sale/?sorting=newest", self.parse, meta=meta)
                yield scrapy.Request(f"https://www.lamudi.com.mx/{state}/{kind}/for-sale/", self.parse, meta=meta)

    def parse(self, response): 
        """ Request a 'url_n' to gather url of the listed properties on the given 'url_n'    
        """
        list_property_url = response.xpath('//div[@id="listings"]/section//a/@href').getall()
        list_property_urls = ["https://www.lamudi.com.mx" + url for url in list_property_url]

        for url in list_property_urls:
            yield scrapy.Request(url, callback=self.parse_property_page, meta=response.meta)
        next_page_url = response.xpath(NEXT_PAGE_XPATH).get()
        if next_page_url:
            yield scrapy.Request(next_page_url, callback=self.parse, meta=response.meta)

    def parse_property_page(self, response):
        json_info = response.xpath(JSON_XPATH).get() or response.xpath(SECONDARY_JSON).get()
        if not json_info:
            retry = response.meta.get("retry", 0)
            if retry < 5:
                time.sleep(10)
                yield response.request.replace(meta={"retry": retry + 1})
            else:
                import pdb; pdb.set_trace()
                return None
        if json_info:
            try:
                info = json.loads(json_info).get("@graph")
                property_info = next((x for x in info if x["@type"] not in ["BreadcrumbList", "Organization"]), {})
            except (json.JSONDecodeError, TypeError):
                json_info = json_info.replace("initAdForm(", "").replace("},\n    );\n", "}")
                property_info = hjson.loads(json_info)
                property_info.update({"address": {"streetAddres": property_info.get("mapData", {}).get("adLocationData", {}).get("address"),
                                                        "addressLocality": property_info.get("mapData", {}).get("adLocationData", {}).get("locality"),
                                                        "addressCountry": {"name": property_info.get("country")}},
                                                "geo": {"latitude": property_info.get("mapData", {}).get("adLocationData", {}).get("coordinates", {}).get("latitude"),
                                                        "longitude": property_info.get("mapData", {}).get("adLocationData", {}).get("coordinates", {}).get("longitude")}
                                                        }
                                                        )
            plot_area = response.xpath(PLOT_AREA_XPATH1).get()
            if not plot_area:
                plot_area = response.xpath(PLOT_AREA_XPATH2).get()
            property = Property()
            property["url"] = response.url
            property["state"] = response.meta["state"]
            property["property_kind"] = response.meta["kind"]
            property["title"] = response.xpath(TITLE_XPATH).get()
            property["price"] = response.xpath(PRICE_XPATH).get()
            property["community_fee"] = response.xpath(COMMUNITY_FEES_XPATH).get()
            property["plot_area"] = plot_area
            property["build_area"] = property_info.get("floorSize", {}).get("value")
            property["rooms"] = property_info.get("numberOfBedrooms")
            property["bathrooms"] = property_info.get("numberOfBathroomsTotal")
            property["description"] = property_info.get("description")
            property["publication_date"] = response.xpath(PUBLICATION_DATE_XPATH).get()
            property["country"] = property_info.get("address", {}).get("addressCountry", {}).get("name")
            property["municipality"] = property_info.get("address", {}).get("addressLocality")
            property["address"] = property_info.get("address", {}).get("streetAddress")
            property["latitude"] = property_info.get("geo", {}).get("latitude")
            property["longitude"] = property_info.get("geo", {}).get("longitude")
            property["image"] = property_info.get("image")
            property["image_urls"] = response.xpath(IMAGES_XPATH).getall()
            property["publisher_name"] = response.xpath(PUBLISHER_NAME_XPATH).get()
            property["publisher_phone"] = response.xpath(PUBLISHER_PHONE_XPATH).get()
            property["publisher_url"] = response.xpath(PUBLISHER_URL).get()
            property["condition"] = response.xpath(CONDITION_XPATH).get()
            property["construction_year"] = response.xpath(CONSTRUCTION_YEAR_XPATH).get()
            property["amenities"] = [amenity["name"] for amenity in property_info.get("amenityFeature", [])]
            property["operation"] = "sale"
            if not property["amenities"]:
                property["amenities"] = [amenity.strip() for amenity in response.xpath(FACILITIES).getall() if amenity.strip()]
            if not property["image"]:
                property["image"] = next(iter(property["image_urls"]), None)
            if not property["description"]:
                property["description"] = response.xpath(SECONDARY_DESCRIPTION).get()
            yield property




