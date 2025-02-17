async function fetchWithRetry(url, retries = 10, delay = 1000) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const response = await fetch(url, {
        headers: {
          Accept: "application/json",
          "Cache-Control": "no-cache",
        },
      });

      if (!response.ok) {
        throw new Error(`HTTP error! Status: ${response.status}`);
      }

      const data = await response.json();
      return data; // Return the data on success
    } catch (error) {
      console.warn(`Attempt ${attempt} failed: ${error.message}`);

      if (attempt < retries) {
        await new Promise((resolve) => setTimeout(resolve, delay)); // Wait before retrying
      } else {
        console.log(`Failed to fetch data after ${retries} attempts`);
      }
    }
  }
}

const checkGates = async () => {
  let oldOne = [
    "AARTIIND",
    "swiggy",
    "uber",
    "iyyangar",
    "ABB",
    "ABBOTINDIA",
    "ABCAPITAL",
    "ABFRL",
    "ACC",
    "ADANIENSOL",
    "ADANIENT",
    "ADANIGREEN",
    "ADANIPORTS",
    "ALKEM",
    "AMBUJACEM",
    "ANGELONE",
    "APLAPOLLO",
    "APOLLOHOSP",
    "APOLLOTYRE",
    "ASHOKLEY",
    "ASIANPAINT",
    "ASTRAL",
    "ATGL",
    "ATUL",
    "AUBANK",
    "AUROPHARMA",
    "AXISBANK",
    "BAJAJ-AUTO",
    "BAJAJFINSV",
    "BAJFINANCE",
    "BALKRISIND",
    "BANDHANBNK",
    "BANKBARODA",
    "BANKINDIA",
    "BATAINDIA",
    "BEL",
    "BERGEPAINT",
    "BHARATFORG",
    "BHEL",
    "BIOCON",
    "BOSCHLTD",
    "BPCL",
    "BSOFT",
    "CAMS",
    "CANBK",
    "CANFINHOME",
    "CDSL",
    "CESC",
    "CGPOWER",
    "CHAMBLFERT",
    "CHOLAFIN",
    "CIPLA",
    "COALINDIA",
    "COFORGE",
    "COLPAL",
    "CONCOR",
    "COROMANDEL",
    "CROMPTON",
    "CUMMINSIND",
    "CYIENT",
    "DABUR",
    "DALBHARAT",
    "DEEPAKNTR",
    "DELHIVERY",
    "DIVISLAB",
    "DIXON",
    "DMART",
    "DRREDDY",
    "EICHERMOT",
    "ESCORTS",
    "EXIDEIND",
    "GAIL",
    "GLENMARK",
    "GMRAIRPORT",
    "GNFC",
    "GODREJCP",
    "GODREJPROP",
    "GRANULES",
    "GRASIM",
    "GUJGASLTD",
    "HAL",
    "HAVELLS",
    "HDFCAMC",
    "HEROMOTOCO",
    "HFCL",
    "HINDALCO",
    "HINDCOPPER",
    "HINDPETRO",
    "HINDUNILVR",
    "HUDCO",
    "ICICIBANK",
    "ICICIGI",
    "ICICIPRULI",
    "IDEA",
    "IDFCFIRSTB",
    "IEX",
    "IGL",
    "INDHOTEL",
    "INDIAMART",
    "INDIANB",
    "INDIGO",
    "INDUSINDBK",
    "INDUSTOWER",
    "INFY",
    "IOC",
    "IPCALAB",
    "IRB",
    "IRCTC",
    "IRFC",
    "ITC",
    "JINDALSTEL",
    "JIOFIN",
    "JKCEMENT",
    "JSL",
    "JSWENERGY",
    "JSWSTEEL",
    "JUBLFOOD",
    "KALYANKJIL",
    "KEI",
    "KOTAKBANK",
    "KPITTECH",
    "LALPATHLAB",
    "LAURUSLABS",
    "LICHSGFIN",
    "LICI",
    "LODHA",
    "LT",
    "LTF",
    "LTIM",
    "LTTS",
    "LUPIN",
    "M&M",
    "M&MFIN",
    "MANAPPURAM",
    "MARICO",
    "MARUTI",
    "MAXHEALTH",
    "MCX",
    "METROPOLIS",
    "MFSL",
    "MGL",
    "MOTHERSON",
    "MPHASIS",
    "MRF",
    "MUTHOOTFIN",
    "NATIONALUM",
    "NAUKRI",
    "NAVINFLUOR",
    "NBCC",
    "NCC",
    "NESTLEIND",
    "NHPC",
    "NMDC",
    "NTPC",
    "NYKAA",
    "OBEROIRLTY",
    "OFSS",
    "OIL",
    "ONGC",
    "PAGEIND",
    "PAYTM",
    "PEL",
    "PERSISTENT",
    "PETRONET",
    "PFC",
    "PHOENIXLTD",
    "PIDILITIND",
    "PIIND",
    "PNB",
    "POLICYBZR",
    "POLYCAB",
    "POONAWALLA",
    "POWERGRID",
    "PRESTIGE",
    "PVRINOX",
    "RAMCOCEM",
    "RBLBANK",
    "RECLTD",
    "RELIANCE",
    "SAIL",
    "SBICARD",
    "SBILIFE",
    "SBIN",
    "SHREECEM",
    "SHRIRAMFIN",
    "SIEMENS",
    "SJVN",
    "SOLARINDS",
    "SONACOMS",
    "SRF",
    "SUNPHARMA",
    "SUNTV",
    "SUPREMEIND",
    "SYNGENE",
    "TATACHEM",
    "TATACOMM",
    "TATACONSUM",
    "TATAELXSI",
    "TATAMOTORS",
    "TATAPOWER",
    "TATASTEEL",
    "TCS",
    "TECHM",
    "TIINDIA",
    "TITAN",
    "TORNTPHARM",
    "TORNTPOWER",
    "TRENT",
    "TVSMOTOR",
    "UBL",
    "ULTRACEMCO",
    "UNIONBANK",
    "UNITDSPR",
    "UPL",
    "VBL",
    "VEDL",
    "VOLTAS",
    "WIPRO",
    "YESBANK",
    "ZOMATO",
    "ZYDUSLIFE",
  ]; //it fetched old data from database.
  //   oldOne = new Set(oldOne);

  const newOne = [];
  const currentData = [];

  const fetchedData = await fetchWithRetry(
    "https://www.nseindia.com/api/master-quote"
  );

  let newStocks = fetchedData.filter((x) => !oldOne.includes(x));
  let removedStocks = oldOne.filter((x) => !fetchedData.includes(x));

  console.log(newStocks, removedStocks);

  //   for (let i of fetchedData) {
  //     if (oldOne.has(i)) {
  //       currentData.push(i);
  //       oldOne.delete(i);
  //     } else {
  //       newOne.push(i);
  //     }
  //   }

  //   console.log(currentData);
  //   console.log(newOne);
  //   console.log(oldOne);
};

checkGates();
