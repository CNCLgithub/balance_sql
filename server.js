const express = require('express');
const { Mutex } = require('async-mutex');
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
const db = new sqlite3.Database(
  './conditions.db',
  sqlite3.OPEN_CREATE | sqlite3.OPEN_READWRITE | sqlite3.OPEN_FULLMUTEX,
  async (err) => {
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

        // Create condition_counts table with last_assigned_condition
        await dbRun(`
        CREATE TABLE IF NOT EXISTS condition_counts (
          session_id TEXT NOT NULL,
          condition_id INTEGER NOT NULL CHECK (condition_id BETWEEN 1 AND ${ncond}),
          user_count INTEGER DEFAULT 0,
          last_assigned_condition INTEGER DEFAULT 0,
          PRIMARY KEY (session_id, condition_id)
        )
      `);
        console.log(`Database setup complete with ${ncond} conditions.`);
      } catch (err) {
        console.error('Error setting up database:', err.message);
        throw err;
      }
    }
  }
);

// Create a mutex to serialize database access
const dbMutex = new Mutex();


// Promisify database methods for async/await
const dbRun = util.promisify(db.run.bind(db));
const dbGet = util.promisify(db.get.bind(db));
const dbAll = util.promisify(db.all.bind(db));


// Endpoint to assign a candidate condition
app.get('/assign-condition', async (req, res) => {
  const prolific_pid = req.query.prolific_pid || 'unknown';
  const session_id = req.query.session_id || 'unknown';
  const release = await dbMutex.acquire();
  try {
    // Begin transaction
    await dbRun('BEGIN TRANSACTION');

    // Check if user is already assigned in this session
    const existingAssignment = await dbGet(`
      SELECT condition_id
      FROM user_conditions
      WHERE user_id = ? AND session_id = ?
    `, [prolific_pid, session_id]);

    if (existingAssignment) {
      // Commit transaction (no changes made)
      await dbRun('COMMIT');
      const conditionId = existingAssignment.condition_id;
      console.log(
        `User ${prolific_pid} in session ${session_id} ` +
        `already assigned to condition ${condition_id}`
      );
      res.json({ condition: conditionId });
      return;
    }

    // Check if session exists in condition_counts
    const sessionExists = await dbGet(`
      SELECT 1
      FROM condition_counts
      WHERE session_id = ?
      LIMIT 1
    `, [session_id]);

    // If session doesn't exist, initialize it with all conditions
    if (!sessionExists) {
      const values = Array.from({ length: ncond }, (_, i) => `('${session_id}', ${i + 1}, 0, 0)`).join(', ');
      await dbRun(`
            INSERT INTO condition_counts (session_id, condition_id, user_count, last_assigned_condition)
            VALUES ${values}
          `);
      console.log(`Initialized session ${session_id} with ${ncond} conditions.`);
    }

    // Get the last assigned condition for the session (use MAX to aggregate across conditions)
    const lastAssigned = await dbGet(`
          SELECT MAX(last_assigned_condition) as last_assigned
          FROM condition_counts
          WHERE session_id = ?
        `, [session_id]);

    const lastAssignedCondition = lastAssigned ? lastAssigned.last_assigned : 0;

    // Select all conditions with the minimum user count for the session
    const minConditions = await dbAll(`
          SELECT condition_id
          FROM condition_counts
          WHERE session_id = ?
          AND user_count = (
            SELECT MIN(user_count)
            FROM condition_counts
            WHERE session_id = ?
          )
          ORDER BY condition_id
        `, [session_id, session_id]);

    if (!minConditions || minConditions.length === 0) {
      throw new Error(`No conditions available for session ${session_id}`);
    }

    // Round-robin: Select the next condition after last_assigned_condition
    let conditionId;
    const conditionIds = minConditions.map(c => c.condition_id);
    if (lastAssignedCondition === 0) {
      conditionId = conditionIds[0]; // Start with the first minimum condition
    } else {
      const lastIndex = conditionIds.indexOf(lastAssignedCondition);
      const nextIndex = lastIndex === -1 || lastIndex === conditionIds.length - 1 ? 0 : lastIndex + 1;
      conditionId = conditionIds[nextIndex];
    }

    // Insert user with pending status
    await dbRun(`
      INSERT INTO user_conditions (user_id, session_id, condition_id, status)
      VALUES (?, ?, ?, 'pending')
    `, [prolific_pid, session_id, conditionId]);

    // Update last_assigned_condition for the session (update all rows to simplify)
    await dbRun(`
          UPDATE condition_counts
          SET last_assigned_condition = ?
          WHERE session_id = ?
        `, [conditionId, session_id]);

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
  } finally {
    release();
  }
});

// Endpoint to confirm a condition assignment
app.post('/confirm-condition', async (req, res) => {
  const { prolific_pid, session_id } = req.body;
  if (!prolific_pid || !session_id) {
    return res.status(400).json({ error: 'Missing subject or session id' });
  }
  const release = await dbMutex.acquire();
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
    console.log(`Confirmed ${prolific_pid} in session ${session_id} for condition ${user.condition_id}`);
    res.json({ status: "success" });
    return;
  } catch (err) {
    // Rollback on error
    await dbRun('ROLLBACK');
    const err_msg =
      `Could not confirm user ${prolific_pid} in session ${session_id}: ` +
      err.message;
    console.error(err_msg);
    res.status(500).json({ error: err_msg });
  } finally {
    release();
  }
});

// Start server
app.listen(port, () => {
  console.log(`Server running at http://${server_address}:${port}`);
});
