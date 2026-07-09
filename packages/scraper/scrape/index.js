import * as cheerio from "cheerio";
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

    const results = await Promise.allSettled(newAds.map(scrapeAdDetails));
    const responses = results
      .filter((r) => r.status === "fulfilled")
      .map((r) => r.value);
    const failures = results.filter((r) => r.status === "rejected");
    if (failures.length) {
      console.warn(`${failures.length} ads failed to scrape:`);
      failures.forEach((f) => console.warn(`  ${f.reason?.message}`));
    }

    console.info("Scraping finished.");

    const listings = responses.map(mapToGeoJson).filter(hasValidCoordinates);

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
    .find(
      { "properties.listingId": { $exists: true } },
      { projection: { "properties.listingId": 1 } }
    )
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

// Ad pages are Next.js; the listing lives in the __NEXT_DATA__ Apollo cache
// under a "RealEstateListing:<id>" key. Returns the same ad shape the
// kijiji-scraper library used to, so mapToGeoJson and the DB schema are unchanged.
const scrapeAdDetails = async (ad) => {
  const res = await fetch(ad.href);
  if (!res.ok) {
    throw new Error(`HTTP ${res.status} fetching ${ad.href}`);
  }
  const $ = cheerio.load(await res.text());
  const raw = $("#__NEXT_DATA__").html();
  if (!raw) {
    throw new Error(`No __NEXT_DATA__ script tag at ${ad.href}`);
  }
  const apollo = JSON.parse(raw).props?.pageProps?.__APOLLO_STATE__ ?? {};
  const listing =
    apollo[`RealEstateListing:${ad.id}`] ??
    Object.values(apollo).find((v) => v?.id === ad.id && v?.title);
  if (!listing) {
    throw new Error(`No listing data in __NEXT_DATA__ at ${ad.href}`);
  }

  return {
    id: listing.id,
    title: listing.title,
    image: listing.imageUrls?.[0],
    images: listing.imageUrls ?? [],
    date: listing.activationDate ? new Date(listing.activationDate) : null,
    url: listing.url ?? ad.href,
    attributes: {
      ...flattenAttributes(listing.attributes),
      type: listing.type,
      // price.amount is in cents; the DB stores dollars
      price: listing.price?.amount != null ? listing.price.amount / 100 : null,
      location: {
        latitude: listing.location?.coordinates?.latitude,
        longitude: listing.location?.coordinates?.longitude,
        mapAddress: listing.location?.address,
      },
    },
  };
};

const flattenAttributes = (attributes) => {
  const attrs = {};
  for (const attr of attributes?.all ?? []) {
    // canonicalValues for numberbathrooms are scaled by 10 ("15" = 1.5);
    // the display values hold the real number
    const values =
      attr.canonicalValues?.length && attr.canonicalName !== "numberbathrooms"
        ? attr.canonicalValues
        : attr.values;
    let value = values.length > 1 ? values : values[0];
    if (typeof value === "string" && value !== "" && !isNaN(value)) {
      value = parseFloat(value);
    }
    attrs[attr.canonicalName] = value;
  }
  return attrs;
};

// documents with malformed geometry would abort the whole insertMany
// because of the 2dsphere index on listing-features
const hasValidCoordinates = (feature) =>
  feature.geometry.coordinates.every(Number.isFinite);

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
