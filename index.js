const express = require("express");
const mysql = require("mysql2/promise");
const { v4: uuidv4 } = require("uuid");
require("dotenv").config();

const app = express();
app.use(express.json());

const db = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  waitForConnections: true,
  connectionLimit: 10
});

/* ==============================
   1️⃣ CREATE USER
============================== */
app.post("/users", async (req, res) => {
  const { userId, user_name, user_email, user_phone } = req.body;

  // Basic validation
  if (!userId || !user_name || !user_email || !user_phone) {
    return res.status(400).json({
      error: "userId, user_name, user_email and user_phone are required"
    });
  }

  try {
    await db.execute(
      `INSERT INTO users 
       (user_id, user_name, user_email, user_phone) 
       VALUES (?, ?, ?, ?)`,
      [userId, user_name, user_email, user_phone]
    );

    return res.status(201).json({
      message: "User created successfully",
      user: {
        userId,
        user_name,
        user_email,
        user_phone
      }
    });

  } catch (err) {

    if (err.code === "ER_DUP_ENTRY") {
      return res.status(409).json({
        error: "User with same userId, email, or phone already exists"
      });
    }

    return res.status(500).json({
      error: "Internal server error"
    });
  }
});


/* ==============================
   2️⃣ CREATE WALLET
============================== */
app.post("/wallets", async (req, res) => {
  const { userId } = req.body;

  if (!userId) {
    return res.status(400).json({ error: "userId is required" });
  }

  try {
    // Ensure user exists
    const [[user]] = await db.execute(
      "SELECT user_id FROM users WHERE user_id = ?",
      [userId]
    );

    if (!user) {
      return res.status(404).json({ error: "User not found" });
    }

    await db.execute(
      "INSERT INTO wallets (user_id, balance) VALUES (?, 0)",
      [userId]
    );

    res.status(201).json({
      message: "Wallet created successfully",
      balance: 0
    });

  } catch (err) {
    if (err.code === "ER_DUP_ENTRY") {
      return res.status(409).json({ error: "Wallet already exists" });
    }
    res.status(500).json({ error: "Internal server error" });
  }
});

/* ==============================
   3️⃣ CREDIT WALLET
============================== */
app.post("/wallets/:userId/credit", async (req, res) => {
  const { userId } = req.params;
  const { amount, referenceId } = req.body;

  if (!amount || amount <= 0) {
    return res.status(400).json({ error: "Amount must be greater than 0" });
  }
  if (!referenceId) {
    return res.status(400).json({ error: "referenceId is required" });
  }

  const conn = await db.getConnection();

  try {
    await conn.beginTransaction();

    await conn.execute(
      `INSERT INTO transactions
      (transaction_id, user_id, type, amount, status, reference_id)
      VALUES (?, ?, 'CREDIT', ?, 'SUCCESS', ?)`,
      [uuidv4(), userId, amount, referenceId]
    );

    const [result] = await conn.execute(
      "UPDATE wallets SET balance = balance + ? WHERE user_id = ?",
      [amount, userId]
    );

    if (result.affectedRows === 0) {
      throw new Error("Wallet not found");
    }

    await conn.commit();

    const [[wallet]] = await db.execute(
      "SELECT balance FROM wallets WHERE user_id = ?",
      [userId]
    );

    res.json({
      message: "Wallet credited successfully",
      balance: wallet.balance
    });

  } catch (err) {
    await conn.rollback();

    if (err.code === "ER_DUP_ENTRY") {
      return res.status(409).json({ error: "Duplicate referenceId" });
    }
    if (err.message === "Wallet not found") {
      return res.status(404).json({ error: "Wallet not found" });
    }
    res.status(500).json({ error: "Credit failed" });

  } finally {
    conn.release();
  }
});

/* ==============================
   4️⃣ DEBIT WALLET
============================== */
app.post("/wallets/:userId/debit", async (req, res) => {
  const { userId } = req.params;
  const { amount, referenceId } = req.body;

  if (!amount || amount <= 0) {
    return res.status(400).json({ error: "Amount must be greater than 0" });
  }
  if (!referenceId) {
    return res.status(400).json({ error: "referenceId is required" });
  }

  const conn = await db.getConnection();

  try {
    await conn.beginTransaction();

    const [[wallet]] = await conn.execute(
      "SELECT balance FROM wallets WHERE user_id = ? FOR UPDATE",
      [userId]
    );

    if (!wallet) {
      throw new Error("Wallet not found");
    }

    if (wallet.balance < amount) {
      await conn.execute(
        `INSERT INTO transactions
        (transaction_id, user_id, type, amount, status, reference_id, failure_reason)
        VALUES (?, ?, 'DEBIT', ?, 'FAILED', ?, 'Insufficient balance')`,
        [uuidv4(), userId, amount, referenceId]
      );

      await conn.commit();
      return res.status(400).json({ error: "Insufficient balance" });
    }

    await conn.execute(
      "UPDATE wallets SET balance = balance - ? WHERE user_id = ?",
      [amount, userId]
    );

    await conn.execute(
      `INSERT INTO transactions
      (transaction_id, user_id, type, amount, status, reference_id)
      VALUES (?, ?, 'DEBIT', ?, 'SUCCESS', ?)`,
      [uuidv4(), userId, amount, referenceId]
    );

    await conn.commit();

    res.json({
      message: "Wallet debited successfully",
      balance: wallet.balance - amount
    });

  } catch (err) {
    await conn.rollback();

    if (err.code === "ER_DUP_ENTRY") {
      return res.status(409).json({ error: "Duplicate referenceId" });
    }
    if (err.message === "Wallet not found") {
      return res.status(404).json({ error: "Wallet not found" });
    }
    res.status(500).json({ error: "Debit failed" });

  } finally {
    conn.release();
  }
});

/* ==============================
   5️⃣ GET WALLET DETAILS
============================== */
app.get("/wallets/:userId", async (req, res) => {
  const { userId } = req.params;

  try {
    const [[wallet]] = await db.execute(
      "SELECT balance FROM wallets WHERE user_id = ?",
      [userId]
    );

    if (!wallet) {
      return res.status(404).json({ error: "Wallet not found" });
    }

    const [transactions] = await db.execute(
      `SELECT transaction_id, type, amount, status,
              reference_id, failure_reason, created_at
       FROM transactions
       WHERE user_id = ?
       ORDER BY created_at DESC
       LIMIT 10`,
      [userId]
    );

    res.json({
      userId,
      balance: wallet.balance,
      transactions
    });

  } catch (err) {
    res.status(500).json({ error: "Internal server error" });
  }
});

/* ==============================
   SERVER START
============================== */
app.listen(process.env.PORT, () => {
  console.log(`Wallet service running on port ${process.env.PORT}`);
});
