require("dotenv").config();
const { MongoClient } = require("mongodb");

const uri = process.env.MONGODB_URI;
const dbName = "CNC_GENIE";
const deviceDataCollectionName = "devicedatas";
const deviceSettingCollectionName = "Device_setting";

// Subtract one day from a date string (YY/MM/DD)
function subtractOneDay(dateString) {
  const [yy, mm, dd] = dateString.split("/").map(Number);
  const date = new Date(2000 + yy, mm - 1, dd);
  date.setDate(date.getDate() - 1);
  return `${(date.getFullYear() % 100)}/${date.getMonth() + 1}/${date.getDate()}`;
}

// Determine shift (morning, night, noshift)
function determineShift(dayShift, nightShift, currentDate, hour, minute) {
  const toMinutes = (timeStr) => {
    const [h, m] = timeStr.split(":").map(Number);
    return h * 60 + m;
  };

  const dayStart = toMinutes(dayShift.start);
  const dayStop = toMinutes(dayShift.stop);
  const nightStart = toMinutes(nightShift.start);
  const nightStop = toMinutes(nightShift.stop);
  const currentTime = hour * 60 + minute;

  let inDayShift = dayStart < dayStop
    ? currentTime >= dayStart && currentTime < dayStop
    : currentTime >= dayStart || currentTime < dayStop;

  let inNightShift = nightStart < nightStop
    ? currentTime >= nightStart && currentTime < nightStop
    : currentTime >= nightStart || currentTime < nightStop;

  if (inNightShift) {
    return currentTime >= nightStart
      ? `${currentDate} ${nightShift.name}`
      : `${subtractOneDay(currentDate)} ${nightShift.name}`;
  } else if (inDayShift) {
    return `${currentDate} ${dayShift.name}`;
  } else {
    return `${currentDate} noshift`;
  }
}

async function processDeviceData() {
  const client = new MongoClient(uri, { connectTimeoutMS: 30000 });

  try {
    await client.connect();
    const db = client.db(dbName);
    const deviceDataCollection = db.collection(deviceDataCollectionName);
    const deviceSettingCollection = db.collection(deviceSettingCollectionName);

    // Fetch documents in batches
    const batchSize = 100; // Adjust for performance
    let skip = 0;
    let processedCount = 0;

    while (true) {
      const documents = await deviceDataCollection
        .find({ $or: [{ ch1_status: { $exists: false } }, { ch1_shift: { $exists: false } }] })
        .skip(skip)
        .limit(batchSize)
        .toArray();

      if (documents.length === 0) break; // No more documents

      console.log(`[${new Date().toISOString()}] Processing ${documents.length} documents (batch).`);
      const bulkOps = [];

      for (const doc of documents) {
        const deviceNo = doc.deviceno;
        const deviceSettingDoc = await deviceSettingCollection.findOne({ deviceno: deviceNo });

        if (!deviceSettingDoc) {
          console.log(`No Device_setting found for device number: ${deviceNo}`);
          continue;
        }

        const updateObj = {};

        // Process status fields
        for (let ch = 1; ch <= 8; ch++) {
          const chData = deviceSettingDoc[`ch${ch}`];
          const chValue = doc[`ch${ch}`];

          if (chValue !== undefined && chData) {
            const statusField = `ch${ch}_status`;
            const { ON_Threshold, LOW_Effeciency_Threshold } = chData;

            if (chValue > LOW_Effeciency_Threshold) {
              updateObj[statusField] = "ON";
            } else if (chValue > ON_Threshold) {
              updateObj[statusField] = "LOW";
            } else {
              updateObj[statusField] = "OFF";
            }
          }
        }

        // Process shift fields
        if (doc.date && doc.time) {
          const [hour, minute] = doc.time.split(":").map(Number);
          const date = doc.date;

          for (let ch = 1; ch <= 8; ch++) {
            const chData = deviceSettingDoc[`ch${ch}`];
            if (chData) {
              const shiftField = `ch${ch}_shift`;
              updateObj[shiftField] = determineShift(
                { name: "morning", start: chData.Morning_shift_start, stop: chData.Morning_shift_end },
                { name: "night", start: chData.Night_shift_start, stop: chData.Night_shift_end },
                date,
                hour,
                minute
              );
            }
          }
        }

        if (Object.keys(updateObj).length > 0) {
          bulkOps.push({
            updateOne: { filter: { _id: doc._id }, update: { $set: updateObj } }
          });
        }
      }

      if (bulkOps.length > 0) {
        await deviceDataCollection.bulkWrite(bulkOps);
        console.log(`Batch updated ${bulkOps.length} documents.`);
        processedCount += bulkOps.length;
      }

      skip += batchSize;
    }

    console.log(`Processing completed. Total updated: ${processedCount}`);
  } catch (err) {
    console.error("Error:", err.message);
  } finally {
    await client.close();
  }
}

// Run every 30 seconds with execution lock
let isRunning = false;
setInterval(async () => {
  if (isRunning) {
    console.log("⏳ Skipping cycle — still processing last batch.");
    return;
  }

  isRunning = true;
  await processDeviceData();
  isRunning = false;
}, 30000);
