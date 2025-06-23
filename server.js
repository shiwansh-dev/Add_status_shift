require("dotenv").config();
const { MongoClient } = require("mongodb");

const uri = process.env.MONGODB_URI;
const dbName = "CNC_GENIE";
const deviceDataCollectionName = "devicedatas";
const deviceSettingCollectionName = "Device_setting";

function subtractOneDay(dateString) {
  const [yy, mm, dd] = dateString.split("/").map(Number);
  const date = new Date(2000 + yy, mm - 1, dd);
  date.setDate(date.getDate() - 1);
  return `${(date.getFullYear() % 100)}/${date.getMonth() + 1}/${date.getDate()}`;
}

function determineShift(dayShift, nightShift, currentDate, hour, minute) {
  const nightStart = nightShift.start.split(":").map(Number);
  const nightStop = nightShift.stop.split(":").map(Number);

  const nightStartTime = nightStart[0] * 60 + nightStart[1];
  const nightStopTime = nightStop[0] * 60 + nightStop[1];
  const currentTime = hour * 60 + minute;

  if (nightStartTime > nightStopTime) {
    if (currentTime >= nightStartTime || currentTime < nightStopTime) {
      return currentTime >= nightStartTime
        ? `${currentDate} ${nightShift.name}`
        : `${subtractOneDay(currentDate)} ${nightShift.name}`;
    } else {
      return `${currentDate} ${dayShift.name}`;
    }
  } else {
    if (currentTime >= nightStartTime && currentTime < nightStopTime) {
      return `${currentDate} ${nightShift.name}`;
    } else {
      return `${currentDate} ${dayShift.name}`;
    }
  }
}

async function processDeviceData() {
  const client = new MongoClient(uri, { connectTimeoutMS: 30000 });
  try {
    await client.connect();
    const db = client.db(dbName);
    const deviceDataCollection = db.collection(deviceDataCollectionName);
    const deviceSettingCollection = db.collection(deviceSettingCollectionName);

    const documents = await deviceDataCollection.find({
      $or: [
        { ch1_status: { $exists: false } },
        { ch1_shift: { $exists: false } }
      ]
    }).toArray();

    console.log(`[${new Date().toISOString()}] Processing ${documents.length} documents.`);

    for (const doc of documents) {
      const deviceNo = doc.deviceno;
      const deviceSettingDoc = await deviceSettingCollection.findOne({ deviceno: deviceNo });

      if (!deviceSettingDoc) {
        console.log(`No Device_setting found for device number: ${deviceNo}`);
        continue;
      }

      const updateObj = {};

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

      if (doc.date && doc.time) {
        const [hour, minute] = doc.time.split(":").map(Number);
        const date = doc.date;

        for (let ch = 1; ch <= 8; ch++) {
          const chData = deviceSettingDoc[`ch${ch}`];
          if (chData) {
            const shiftField = `ch${ch}_shift`;

            const shiftValue = determineShift(
              {
                name: "morning",
                start: chData.Morning_shift_start,
                stop: chData.Morning_shift_end
              },
              {
                name: "night",
                start: chData.Night_shift_start,
                stop: chData.Night_shift_end
              },
              date, hour, minute
            );

            updateObj[shiftField] = shiftValue;
          }
        }
      }

      if (Object.keys(updateObj).length > 0) {
        await deviceDataCollection.updateOne({ _id: doc._id }, { $set: updateObj });
        console.log(`Updated _id: ${doc._id} =>`, updateObj);
      }
    }
  } catch (err) {
    console.error("Error:", err.message);
  } finally {
    await client.close();
  }
}

// 🔁 Run every 30 seconds with execution lock
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
