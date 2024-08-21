const express = require('express');
const { MongoClient } = require('mongodb');
const cors = require('cors');

const app = express();
const port = 5000;
const connectionString = 'mongodb+srv://db_user_read:LdmrVA5EDEv4z3Wr@cluster0.n10ox.mongodb.net/RQ_Analytics?retryWrites=true&w=majority';

// Middleware
app.use(cors());
app.use(express.urlencoded({ extended: true }));
app.use(express.json());

// MongoDB Client Initialization
let db;
MongoClient.connect(connectionString, { useNewUrlParser: true, useUnifiedTopology: true })
  .then(client => {
    db = client.db('RQ_Analytics'); // Ensure the database name is a string
    console.log('Connected to MongoDB');
  })
  .catch(err => {
    console.error('Failed to connect to the database. Error:', err);
    process.exit(1);
  });

// Endpoint to get total sales over time
app.get('/api/sales/total', async (req, res) => {
  if (!db) {
    return res.status(500).json({ error: 'Database not initialized' });
  }

  const interval = req.query.interval || 'day';

  const pipeline = [
    {
      $addFields: {
        created_at: {
          $dateFromString: { dateString: "$created_at" }
        }
      }
    },
    {
      $group: {
        _id: {
          $dateTrunc: {
            date: "$created_at",
            unit: interval
          }
        },
        totalSales: { $sum: "$total_price_set.shop_money" }
      }
    },
    { $sort: { "_id": 1 } }
  ];

  try {
    const salesData = await db.collection('shopifyOrders').aggregate(pipeline).toArray();
    res.json(salesData);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});


// Endpoint to get new customers over time
app.get('/api/customers/new', async (req, res) => {
  if (!db) {
    return res.status(500).json({ error: 'Database not initialized' });
  }

  const interval = req.query.interval || 'day';

  const pipeline = [
    {
      $addFields: {
        created_at: {
          $dateFromString: { dateString: "$created_at" }
        }
      }
    },
    {
      $group: {
        _id: {
          $dateTrunc: {
            date: "$created_at",
            unit: interval
          }
        },
        newCustomers: { $sum: 1 }
      }
    },
    { $sort: { "_id": 1 } }
  ];

  try {
    const customerData = await db.collection('shopifyCustomers').aggregate(pipeline).toArray();
    res.json(customerData);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});


// Endpoint to get number of repeat customers
app.get('/api/customers/repeat', async (req, res) => {
  if (!db) {
    return res.status(500).json({ error: 'Database not initialized' });
  }

  const interval = req.query.interval || 'day';

  const pipeline = [
    {
      $lookup: {
        from: 'shopifyOrders',
        localField: '_id',
        foreignField: 'customer_id',
        as: 'orders'
      }
    },
    { $unwind: '$orders' },
    {
      $addFields: {
        'orders.created_at': {
          $dateFromString: { dateString: "$orders.created_at" }
        }
      }
    },
    {
      $group: {
        _id: {
          $dateTrunc: {
            date: "$orders.created_at",
            unit: interval
          }
        },
        customers: { $addToSet: "$_id" }
      }
    },
    {
      $project: {
        _id: 1,
        repeatCustomers: { $size: "$customers" }
      }
    },
    { $sort: { "_id": 1 } }
  ];

  try {
    const repeatCustomerData = await db.collection('shopifyCustomers').aggregate(pipeline).toArray();
    res.json(repeatCustomerData);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});


// Endpoint to get geographical distribution of customers
app.get('/api/customers/geographical', async (req, res) => {
  if (!db) {
    return res.status(500).json({ error: 'Database not initialized' });
  }

  try {
    const pipeline = [
      {
        $group: {
          _id: "$default_address.city",
          count: { $sum: 1 }
        }
      },
      { $sort: { count: -1 } }
    ];

    const geoData = await db.collection('shopifyCustomers').aggregate(pipeline).toArray();
    res.json(geoData);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});


// Endpoint to get customer lifetime value by cohorts
app.get('/api/customers/lifetime-value', async (req, res) => {
  if (!db) {
    return res.status(500).json({ error: 'Database not initialized' });
  }

  try {
    const pipeline = [
      {
        $lookup: {
          from: 'shopifyOrders',
          localField: '_id',
          foreignField: 'customer_id',
          as: 'orders'
        }
      },
      {
        $addFields: {
          firstOrderDate: { $min: "$orders.created_at" },
          totalSpent: { $sum: "$orders.total_price_set.shop_money" }
        }
      },
      {
        $addFields: {
          firstOrderDate: {
            $dateFromString: { dateString: "$firstOrderDate" }
          }
        }
      },
      {
        $group: {
          _id: {
            $dateTrunc: {
              date: "$firstOrderDate",
              unit: "month"
            }
          },
          totalLifetimeValue: { $sum: "$totalSpent" }
        }
      },
      { $sort: { "_id": 1 } }
    ];

    const cohortData = await db.collection('shopifyCustomers').aggregate(pipeline).toArray();
    res.json(cohortData);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});


app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
});
