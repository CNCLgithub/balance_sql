const express = require('express');
const sqlite3 = require('sqlite3').verbose();
const cors = require('cors');
const util = require('util');
const app = express();

// const server_address = "78.141.233.16";
const server_address = "localhost";
const port = 3001;

// Enable CORS for JATOS frontend
app.use(cors());
app.use(express.json());


const ncond = 12;

// Initialize SQLite database
const db = new sqlite3.Database('./conditions.db', async (err) => {
  if (err) {
    console.error('Error opening database:', err.message);
  } else {
    console.log('Connected to SQLite database.');
    try {
      // Create user_conditions table with session_id and dynamic CHECK constraint
      await dbRun(`
            CREATE TABLE IF NOT EXISTS user_conditions (
                user_id INTEGER NOT NULL,
                session_id TEXT NOT NULL,
                condition_id INTEGER NOT NULL CHECK (condition_id BETWEEN 1 AND ${ncond}),
                status TEXT NOT NULL CHECK (status IN ('pending', 'completed')),
                assigned_at TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (user_id, session_id)
            )
    `);

      // Create condition_counts table with session_id and composite primary key
      await dbRun(`
            CREATE TABLE IF NOT EXISTS condition_counts (
                session_id TEXT NOT NULL,
                condition_id INTEGER NOT NULL CHECK (condition_id BETWEEN 1 AND ${ncond}),
                user_count INTEGER DEFAULT 0,
                PRIMARY KEY (session_id, condition_id)
            )
        `);
      console.log(`Database setup complete with ${ncond} conditions.`);
    } catch (err) {
      console.error('Error setting up database:', err.message);
      throw err;
    }
  }
});


// Promisify database methods for async/await
const dbRun = util.promisify(db.run.bind(db));
const dbGet = util.promisify(db.get.bind(db));


// Endpoint to assign a candidate condition
app.get('/assign-condition', async (req, res) => {
  const prolific_pid = req.query.prolific_pid || 'unknown';
  const session_id = req.query.session_id || 'unknown';
  try {
    // Begin transaction
    await dbRun('BEGIN TRANSACTION');

    // Check if session exists in condition_counts
    const sessionExists = await dbGet(`
      SELECT 1
      FROM condition_counts
      WHERE session_id = ?
      LIMIT 1
    `, [session_id]);

    // If session doesn't exist, initialize it with all conditions
    if (!sessionExists) {
      const values = Array.from({ length: ncond }, (_, i) => `('${session_id}', ${i + 1}, 0)`).join(', ');
      await dbRun(`
        INSERT INTO condition_counts (session_id, condition_id, user_count)
        VALUES ${values}
      `);
      console.log(`Initialized session ${session_id} with ${ncond} conditions.`);
    }

    // Select condition with minimum completed users for the session
    const minCondition = await dbGet(`
      SELECT condition_id
      FROM condition_counts
      WHERE session_id = ?
      AND user_count = (
        SELECT MIN(user_count)
        FROM condition_counts
        WHERE session_id = ?
      )
      LIMIT 1
    `, [session_id, session_id]);

    if (!minCondition) {
      throw new Error(`No conditions available for session ${session_id}`);
    }

    const conditionId = minCondition.condition_id;

    // Insert user with pending status
    await dbRun(`
      INSERT INTO user_conditions (user_id, session_id, condition_id, status)
      VALUES (?, ?, ?, 'pending')
    `, [prolific_pid, session_id, conditionId]);

    // Commit transaction
    await dbRun('COMMIT');
    console.log(`User ${prolific_pid} in session ${session_id} assigned to condition ${conditionId} as pending`);
    // Return candidate condition
    res.json({ condition: conditionId });
  } catch (err) {
    // Rollback on error
    await dbRun('ROLLBACK');
    console.error(`Error assigning user ${prolific_pid} in session ${session_id}:`, err);
    res.status(500).json({ error: `Error assigning user ${prolific_pid} in session ${session_id}:` });
  }
});

// Endpoint to confirm a condition assignment
app.post('/confirm-condition', async (req, res) => {
  const { prolific_pid, session_id } = req.body;
  if (!prolific_pid || !session_id) {
    return res.status(400).json({ error: 'Missing subject or session id' });
  }

  const maxRetries = 3;
  let attempt = 0;

  while (attempt < maxRetries) {
    try {
      // Begin transaction
      await dbRun('BEGIN TRANSACTION');

      // Verify user exists and is pending in the session
      const user = await dbGet(`
        SELECT condition_id, status
        FROM user_conditions
        WHERE user_id = ? AND session_id = ?
      `, [prolific_pid, session_id]);

      if (!user) {
        throw new Error(`User ${prolific_pid} not found in session ${session_id}`);
      }
      if (user.status === 'completed') {
        throw new Error(`User ${prolific_pid} task already completed in session ${session_id}`);
      }

      // Update user status to completed
      await dbRun(`
        UPDATE user_conditions
        SET status = 'completed'
        WHERE user_id = ? AND session_id = ?
      `, [prolific_pid, session_id]);

      // Increment condition count for the session
      await dbRun(`
        UPDATE condition_counts
        SET user_count = user_count + 1
        WHERE session_id = ? AND condition_id = ?
      `, [session_id, user.condition_id]);

      // Commit transaction
      await dbRun('COMMIT');
      console.log(`User ${prolific_pid} task completed in session ${session_id}, condition ${user.condition_id} count updated`);
      res.json({ status: "success" });
      return;
    } catch (err) {
      // Rollback on error
      await dbRun('ROLLBACK');
      attempt++;
      if (err.code === 'SQLITE_BUSY' && attempt < maxRetries) {
        console.warn(`SQLITE_BUSY on attempt ${attempt} for user ${prolific_pid} in session ${session_id}, retrying...`);
        // Wait 100ms before retrying
        await new Promise(resolve => setTimeout(resolve, 100));
        continue;
      }
      console.error(`Error completing task for user ${prolific_pid} in session ${session_id} after ${attempt} attempts:`, err.message);
      res.status(500).json({ error: "Could not confirm assignment" });
    }
  }
  res.status(500).json({ error: `Error completing task for user ${prolific_pid} in session ${session_id} after ${attempt} attempts: ${err.message}` });
  throw new Error(`Failed to complete task for user ${prolific_pid} in session ${session_id} after ${maxRetries} attempts`);
});

// Start server
// Start server
app.listen(port, () => {
  console.log(`Server running at http://${server_address}:${port}`);
});
