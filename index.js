const express = require('express');
const app = express();

require('dotenv').config();
const redis = require('redis');
const amqp = require('amqplib');
const pg = require('pg');

let redisClient;
let rabbitmqChannel;

const queueName = 'adding';


(async () => {
  redisClient = redis.createClient({
    password: process.env.REDIS_PASSWORD,
    socket: {
        host: process.env.REDIS_HOST,
        port: process.env.REDIS_PORT
    }
  });

  redisClient.on("error", (error) => console.error(`Error : ${error}`));

  await redisClient.connect();
})();

(async () => {
    try {
      const connection = await amqp.connect(process.env.RABBIT_STRING);
      rabbitmqChannel = await connection.createChannel();
    } catch (error) {
      console.error('Error while connecting to RabbitMQ:', error);
    }
})();




app.use(express.json());
const connectionString = process.env.PG_STRING;

const pool = new pg.Pool({ connectionString });

app.post('/', async (req, res) => {
    try {
      const { title, text } = req.body;
      if (!title || !text) res.status(404).json({ error: 'Missing fields' })
      
      await rabbitmqChannel.assertQueue(queueName);
      rabbitmqChannel.sendToQueue(queueName, Buffer.from(JSON.stringify(req.body)));  
      res.json('Added to queue');
    } catch (error) {
      console.error('Error creating post:', error);
      res.status(500).json({ error: 'An error occurred while creating the post.' });
    }
});

app.post('/process', async (req, res) => {
    rabbitmqChannel.consume(queueName, async (message) => {
        if (message !== null) {
            const content = JSON.parse(message.content);
            console.log('Message:', content);

            const query = 'INSERT INTO posts (title, text) VALUES ($1, $2) RETURNING *';
            const values = [content.title, content.text];
  
            await pool.query(query, values);

            rabbitmqChannel.ack(message);
        }
    });

    res.status(200).send('Process a queue');
})
    
  
app.get('/', async (req, res) => {
    const limit = req.body.limit || 5;
    const page = req.body.page || 1;
    try {
        const query = 'SELECT * FROM posts LIMIT $1 OFFSET $2';
        const values = [limit, limit * (page - 1)]
        const result = await pool.query(query, values);
        res.json({
            data: result.rows,
            page,
            quantity: limit
        });
    } catch (error) {
        console.error('Error fetching posts:', error);
        res.status(500).json({ error: 'An error occurred while fetching posts.' });
    }
});

app.get('/:id', async (req, res) => {
    const postId = req.params.id;
    let result;
    let isCached = false;
    const query = 'SELECT * FROM posts WHERE id = $1';

    try {
        const cacheResult = await redisClient.get(postId);
        if(cacheResult) {
            isCached = true;
            result = JSON.parse(cacheResult);
        }
        else {
            result = await pool.query(query, [postId]);
            result = result.rows[0]
            if(result.rowCount === 0) {
                throw 'An empty object'
            }
            await redisClient.set(postId, JSON.stringify(result));
        }

        if (result.rowCount === 0) {
            res.status(404).json({ error: 'Post not found.' });
        } else {
            res.send({
                data: result,
                isCached
            })
        }
    } catch (error) {
        console.error('Error fetching post by ID:', error);
        res.status(500).json({ error: 'An error occurred while fetching the post.' });
    }
});

app.listen(5001, () => {
    console.log('Server is running on http://localhost:5001');
})
