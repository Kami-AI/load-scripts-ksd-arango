import { Database, aql } from 'arangojs';
import * as parse from 'csv-parse/lib/sync';
import * as fs from 'fs';
import axios, { AxiosError } from 'axios';
​
const db = new Database({
  url: 'http://localhost:8529',
  databaseName: 'knowledge_admin_dmo',
  auth: { username: 'root', password: '' },
});
​
const getAll = (model: string) => {
  const collection = db.collection(model);
  return db.query(aql`
  FOR d in ${collection}
    RETURN d
  `);
};
​
const chi = (value: string) => {
  if (!value) {
    return null;
  }
  return {
    default_lang_code: 'zh-tw',
    default_value: value,
    translations: {
      'zh-tw': value,  
    },
  }
};
​
const m = (chiStr: string, engStr: string) => {
  if (!chiStr && !engStr) {
    return null;
  }
  if (!engStr) {
    return chi(chiStr);
  }
  if (!chiStr) {
    return {
      default_lang_code: 'en-us',
      default_value: engStr,
      translations: {
        'en-us': engStr,  
      },
    };
  }
  return {
    default_lang_code: 'zh-tw',
    default_value: chiStr,
    translations: {
      'zh-tw': chiStr,
      'en-us': engStr,  
    },
  };
};
​
const countryCache = {};
​
const buildCountryCache = async () => {
  console.log('building country cache...');
  const cursor = await getAll('Country');
  for await (const doc of cursor) {
    const name = doc.name.default_value;
    const id = doc._id;
    countryCache[name] = id;
  }
  console.log('built country cache.');
};
​
const getCountry = (name: string) => {
  return countryCache[name];
};
​
const adminAreaCache = {};
​
const buildAdminAreaCache = async () => {
  console.log('building admin area cache...');
  const cursor = await getAll('AdminArea');
  for await (const doc of cursor) {
    const name = doc.name.default_value;
    const id = doc._id;
    adminAreaCache[name] = id;
  }
  console.log('built admin area cache.');
};
​
const getAdminArea = (name: string) => {
  return adminAreaCache[name];
};
​
const industryCache = {};
​
const buildIndustryCache = async () => {
  console.log('building industry cache...');
  const cursor = await getAll('Industry');
  for await (const doc of cursor) {
    const name = doc.name.default_value;
    const id = doc._id;
    industryCache[name] = id;
  }
  console.log('built industry cache.');
};
​
const getIndustry = (name: string) => {
  return industryCache[name];
};
​
const placeTypeCache = {};
​
const buildPlaceTypeCache = async () => {
  console.log('building place type cache...');
  const cursor = await getAll('PlaceType');
  for await (const doc of cursor) {
    const name = doc.name.default_value;
    const id = doc._id;
    placeTypeCache[name] = id;
  }
  console.log('built place type cache.');
};
​
const getPlaceType = (name: string) => {
  return placeTypeCache[name];
};
​
const featureCache = {};
​
const buildFeatureCache = async () => {
  console.log('building feature cache...');
  const cursor = await getAll('Feature');
  for await (const doc of cursor) {
    const name = doc.name.default_value;
    const id = doc._id;
    featureCache[name] = id;
  }
  console.log('built feature cache.');
};
​
const getFeature = (name: string) => {
  return featureCache[name];
};
​
const paymentMethodCache = {};
​
const buildPaymendMethodCache = async () => {
  console.log('building payment method cache...');
  const cursor = await getAll('PaymentMethod');
  for await (const doc of cursor) {
    const name = doc.name.default_value;
    const id = doc._id;
    paymentMethodCache[name] = id;
  }
  console.log('built payment method cache.');
};
​
const getPaymentMethod = (name: string) => {
  return paymentMethodCache[name];
};
​
const mealTypeCache = {};
​
const buildMealTypeCache = async () => {
  console.log('building meal type cache...');
  const cursor = await getAll('FnbMealType');
  for await (const doc of cursor) {
    const name = doc.name.default_value;
    const id = doc._id;
    mealTypeCache[name] = id;
  }
  console.log('built meal type cache.');
};
​
const getMealType = (name: string) => {
  return mealTypeCache[name];
};
​
const checkDoc = (row: any) => {
  return row['store_name_zh-tw'] && row.latitude && row.longitude && getAdminArea(row['admin_lv2.name']) && getIndustry(row['industry.name']);
};
​
const transformToDoc = (row: { [key: string]: string }) => {
  for (const k in row) {
    row[k] = row[k].trim();
  }
  const doc: any = {};
  if (row['From'] === 'Google') {
    doc.proposed_key = row['google_place_id'];
  }
​
  doc.name = m(row['store_name_zh-tw'], row['store_name_en']);
  doc.address = m(row['address_zh-tw'], row['address_en']);
  doc.human_address = chi(row['human_address'])
  doc.location = {
    type: 'Point',
    coordinates: [
      parseFloat(row.longitude),
      parseFloat(row.latitude),
    ]
  };
  doc.admin_area = getAdminArea(row['admin_lv2.name']);
  if (row['telephone']) {
    doc.contact = {
      phones: [{
        country_code: '+886',
        phone: row['telephone']
      }],
    };
  }
  if (row['business_hour.zh-tw']) {
    doc.business_hour_text = chi(row['business_hour.zh-tw']);
  }
  doc.description = m(row['description.zh-tw'], row['description.en']);
  const allPlaceTypes = row['place_type.name']
    .split(',')
    .filter(e => e.trim())
    .map(e => getPlaceType(e))
    .filter(e => e);
  let placeTypes = null;
  if (allPlaceTypes.length > 0) {
    placeTypes = {
      primary: {
        place_type: allPlaceTypes[0],
      },
      secondary: allPlaceTypes.slice(1).map(e => ({ place_type: e })),
    };
  }
  const features = row['place_feature.name']
    .split(',')
    .filter(e => e.trim())
    .map(e => getFeature(e))
    .filter(e => e);
  doc.industries = {
    primary: {
      industry: getIndustry(row['industry.name']),
      place_types: placeTypes,
      features,
    },
  };
​
  const paymentMethods = row['payment_type.name']
    .split(',')
    .filter(e => e.trim())
    .map(e => getPaymentMethod(e))
    .filter(e => e);
  doc.payment_methods = paymentMethods;
  doc.default_display_language = 'zh-tw';
​
  return doc;
}
​
(async () => {
  await buildCountryCache();
  await buildAdminAreaCache();
  await buildIndustryCache();
  await buildPlaceTypeCache();
  await buildFeatureCache();
  await buildPaymendMethodCache();
  await buildMealTypeCache();
​
  const input = parse(fs.readFileSync('./places.csv'), {
    columns: true,
    skip_empty_lines: true
  });
​
  for (const row of input) {
    if (checkDoc(row)) {
      try {
        const doc = transformToDoc(row);
        console.log(doc.name.default_value);
        const response = await axios.post('http://localhost:8000/api/dmo/places', doc);
        const placeID = response.data._id;
        if (row['industry.name'] === '餐飲業') {
          const mealTypes = row['meal_type.name']
            .split(',')
            .filter(e => e.trim())
            .map(e => getMealType(e))
            .filter(e => e);
          const body = {
            discriminator: 'fnb',
            place: placeID,
            fnb: {
              meal_types: mealTypes,
            },
          };
          await axios.post('http://localhost:8000/api/dmo/business', body);
        }
      } catch (error) {
        console.log(error.response.data);
        break;
      }
    }
  }
})();