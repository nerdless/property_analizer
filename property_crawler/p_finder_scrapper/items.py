# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field


class Property(Item):
    title = Field()
    price = Field()
    currency = Field()
    community_fee = Field()
    plot_area = Field()
    build_area = Field()
    rooms = Field()
    bathrooms = Field()
    description = Field()
    url = Field()
    publication_date = Field()
    property_kind = Field()
    country = Field()
    state = Field()
    municipality = Field()
    postal_code = Field()
    address = Field()
    latitude = Field()
    longitude = Field()
    image = Field()
    image_urls = Field()
    publisher_name = Field()
    phone = Field()
    email = Field()
    condition = Field()
    construction_year = Field()
    amenities = Field()
    operation = Field()
