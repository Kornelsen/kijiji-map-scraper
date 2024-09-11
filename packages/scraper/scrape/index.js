import { ScraperType, categories, locations, search } from "kijiji-scraper";
import { MongoClient, ServerApiVersion } from "mongodb";

const password = encodeURIComponent(process.env.DB_PASSWORD);
const uri = `mongodb+srv://${process.env.DB_USER}:${password}@${process.env.DB_URI}`;

const client = new MongoClient(uri, {
  serverApi: {
    version: ServerApiVersion.v1,
    strict: true,
    deprecationErrors: true,
  },
});

export async function main() {
  try {
    console.info("Starting scraping process.");
    const resp = await search(
      {
        locationId: locations.ONTARIO.TORONTO_GTA.CITY_OF_TORONTO.id,
        categoryId: categories.REAL_ESTATE.FOR_RENT.LONG_TERM_RENTALS.id,
        sortByName: "dateDesc",
        minResults: 60,
      },
      { scraperType: ScraperType.HTML }
    );

    console.info("Scraping finished.");

    const listings = resp.map(mapToListing);

    const db = client.db("kijiji-map");

    const result = await db.collection("pending-listings").insertMany(listings);
    console.info(`${result.insertedCount} pending listings were found.`);
    console.info("Running agregation piepleine.");

    const beforeCount = await db.collection("listings").countDocuments();

    await db.collection("pending-listings").aggregate(mergePipeline).toArray();
    console.info("Aggregation pipeline finished.");
    const afterCount = await db.collection("listings").countDocuments();
    console.info(`${afterCount - beforeCount} new listings were inserted.`);

    console.info("Deleting pending listings.");
    db.collection("pending-listings").deleteMany({});
    console.info("Process finished successfully.");

    return Response.json({ success: true });
  } catch (error) {
    console.error(error);
    return Response.error();
  }
}

const mergePipeline = [
  {
    $merge: {
      into: "listings",
      on: "listingId",
      whenMatched: "keepExisting",
      whenNotMatched: "insert",
    },
  },
];

const mapToListing = (ad) => {
  return {
    listingId: ad.id,
    title: ad.title,
    image: ad.image,
    images: ad.images,
    address: ad.attributes.location.mapAddress,
    date: ad.date,
    location: {
      coordinates: [
        ad.attributes.location.longitude,
        ad.attributes.location.latitude,
      ],
      type: "Point",
    },
    price: ad.attributes.price,
    bedrooms: ad.attributes.numberbedrooms,
    bathrooms: ad.attributes.numberbathrooms,
    url: ad.url,
    sqft: ad.attributes.areainfeet,
    attributes: ad.attributes,
  };
};
