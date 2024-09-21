import * as cheerio from "cheerio";
import { Ad } from "kijiji-scraper";
import { MongoClient, ServerApiVersion } from "mongodb";

const password = encodeURIComponent(process.env.DB_PASSWORD);
const uri = `mongodb+srv://${process.env.DB_USER}:${password}@${process.env.DB_URI}`;

const ADS_URL =
  "https://www.kijiji.ca/b-apartments-condos/city-of-toronto/c37l1700273?sort=dateDesc";

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

    const idSet = await getExistingAdIds();
    const newAds = (await scrapeRecentAds()).filter((ad) => !idSet.has(ad.id));

    console.info(`Found ${newAds.length} new ads to scrape.`);

    const promises = newAds.map((ad) => Ad.Get(ad.href));
    const responses = await Promise.all(promises);

    console.info("Scraping finished.");

    const listings = responses.map(mapToGeoJson);

    const db = client.db("kijiji-map");

    if (!listings.length) {
      console.info("No listings were found by scraper.");
      return Response.json({ success: true });
    }

    const result = await db.collection("pending-listings").insertMany(listings);
    console.info(`${result.insertedCount} pending listings were found.`);
    console.info("Running agregation piepleine.");

    const beforeCount = await db
      .collection("listing-features")
      .countDocuments();

    await db.collection("pending-listings").aggregate(mergePipeline).toArray();
    console.info("Aggregation pipeline finished.");
    const afterCount = await db.collection("listing-features").countDocuments();
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
      into: "listing-features",
      on: "properties.listingId",
      whenMatched: "keepExisting",
      whenNotMatched: "insert",
    },
  },
];

const getExistingAdIds = async () => {
  const db = client.db("kijiji-map");
  const ids = await db
    .collection("listing-features")
    .find({}, { projection: { "properties.listingId": 1 } })
    .toArray();
  const idsSet = new Set(ids.map((ad) => ad.properties.listingId));
  return idsSet;
};

const scrapeRecentAds = async () => {
  const page = await fetch(ADS_URL);
  const html = await page.text();
  const $ = cheerio.load(html);
  const links = $('[data-testid="listing-link"]');
  const ads = [];
  links.each((i, div) => {
    const href = $(div).attr("href");
    const id = href.split("/").pop();
    ads.push({
      id,
      href,
    });
  });
  return ads;
};

const mapToGeoJson = (ad) => {
  return {
    type: "Feature",
    geometry: {
      type: "Point",
      coordinates: [
        ad.attributes.location.longitude,
        ad.attributes.location.latitude,
      ],
    },
    properties: {
      listingId: ad.id,
      title: ad.title,
      image: ad.image,
      images: ad.images,
      address: ad.attributes.location.mapAddress,
      date: ad.date,
      price: ad.attributes.price,
      bedrooms: ad.attributes.numberbedrooms,
      bathrooms: ad.attributes.numberbathrooms,
      url: ad.url,
      sqft: ad.attributes.areainfeet,
      attributes: ad.attributes,
    },
  };
};
