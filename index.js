// 🔥 EMI MONITOR - Embedded Scheduler + Job Queue Architecture
// Replaces cron jobs with in-process scheduler and persistent job queue
// Uses JSON files for state persistence (no database required)

const express = require("express");
const ethers = require("ethers");
const fs = require("fs").promises;
const path = require("path");
const app = express();
app.use(express.json());

console.log(`\n${"=".repeat(70)}`);
console.log(`🚀 EMI MONITOR - EMBEDDED SCHEDULER + JOB QUEUE STARTING`);
console.log(`${"=".repeat(70)}\n`);

// ============================================================================
// CONFIGURATION & CONSTANTS
// ============================================================================

const DATA_DIR = path.join(__dirname, "data");
const STORAGE_FILE = path.join(DATA_DIR, "storage.json");
const QUEUE_FILE = path.join(DATA_DIR, "queue.json");
const LOGS_FILE = path.join(DATA_DIR, "activity.log");

// Scheduler intervals (in milliseconds)
const PAYMENT_DETECTION_INTERVAL = 5000;  // Check for incoming payments every 5s
const EMI_COLLECTION_INTERVAL = 30000;     // Check for due EMI payments every 30s
const QUEUE_PROCESSING_INTERVAL = 2000;    // Process job queue every 2s

// ============================================================================
// CORS MIDDLEWARE
// ============================================================================

app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
  res.header("Access-Control-Allow-Headers", "Content-Type, Authorization");

  if (req.method === "OPTIONS") {
    return res.sendStatus(200);
  }

  next();
});

console.log(`✅ CORS Middleware: ENABLED\n`);

// ============================================================================
// ADMIN KEY VALIDATION
// ============================================================================

const adminKey = process.env.ADMIN_PRIVATE_KEY;
if (adminKey) {
  console.log(`✅ ADMIN_PRIVATE_KEY: SET (${adminKey.slice(0, 10)}...)`);
} else {
  console.error(`❌ ADMIN_PRIVATE_KEY: NOT SET`);
  console.error(`   ⚠️  Payment detection will work, but contract activation will FAIL\n`);
}

console.log(`${"=".repeat(70)}\n`);

// ============================================================================
// RPC ENDPOINTS
// ============================================================================

const RPC_ENDPOINTS = {
  11155111: process.env.SEPOLIA_RPC || "https://sepolia.infura.io/v3/3b801e8b02084ba68f55b81b9209c916",
  1: process.env.MAINNET_RPC || "https://mainnet.infura.io/v3/3b801e8b02084ba68f55b81b9209c916",
  80001: process.env.MUMBAI_RPC || "https://rpc-mumbai.maticvigil.com/",
  137: process.env.POLYGON_RPC || "https://polygon-rpc.com/",
  97: process.env.BSC_TESTNET_RPC || "https://data-seed-prebsc-1-s1.binance.org:8545",
  56: process.env.BSC_MAINNET_RPC || "https://bsc-dataseed1.binance.org",
};

// ============================================================================
// CONTRACT ABI
// ============================================================================

const CONTRACT_ABI_FULL = [
  "event DirectPaymentReceived(uint256 indexed planId, address indexed receiver, address indexed sender, uint256 amount, uint256 timestamp)",
  "function plans(uint256) view returns (address,address,uint256,uint256,uint256,uint256,uint256,bool,bool)",
  "function planCount() view returns (uint256)",
  "function collectPayment(uint256 planId) external payable",
  "function activatePlanRaw(uint256 planId, address sender) external",
];

// ============================================================================
// FILE SYSTEM UTILITIES - JSON Storage Management
// ============================================================================

/**
 * Initialize data directory and JSON storage files
 * Creates directory and empty JSON files if they don't exist
 */
async function initializeStorage() {
  try {
    // Create data directory if it doesn't exist
    await fs.mkdir(DATA_DIR, { recursive: true });
    
    // Initialize storage.json (monitors state)
    try {
      await fs.access(STORAGE_FILE);
    } catch {
      await fs.writeFile(STORAGE_FILE, JSON.stringify({}, null, 2));
      console.log(`📁 Created storage.json`);
    }
    
    // Initialize queue.json (job queue)
    try {
      await fs.access(QUEUE_FILE);
    } catch {
      await fs.writeFile(QUEUE_FILE, JSON.stringify([], null, 2));
      console.log(`📁 Created queue.json`);
    }
    
    // Initialize activity.log
    try {
      await fs.access(LOGS_FILE);
    } catch {
      await fs.writeFile(LOGS_FILE, `Activity Log - Started ${new Date().toISOString()}\n`);
      console.log(`📁 Created activity.log\n`);
    }
    
    console.log(`✅ Storage initialized successfully\n`);
  } catch (error) {
    console.error(`❌ Storage initialization failed:`, error.message);
    process.exit(1);
  }
}

/**
 * Read storage.json (monitor state)
 * Returns: { receiver: { planId, chainId, contract, active, sender, ... } }
 */
async function readStorage() {
  try {
    const data = await fs.readFile(STORAGE_FILE, "utf8");
    return JSON.parse(data);
  } catch (error) {
    console.error(`❌ Failed to read storage.json:`, error.message);
    return {};
  }
}

/**
 * Write to storage.json atomically
 * Prevents corruption during concurrent writes
 */
async function writeStorage(data) {
  try {
    const tempFile = STORAGE_FILE + ".tmp";
    await fs.writeFile(tempFile, JSON.stringify(data, null, 2));
    await fs.rename(tempFile, STORAGE_FILE);
  } catch (error) {
    console.error(`❌ Failed to write storage.json:`, error.message);
  }
}

/**
 * Read queue.json (job queue)
 * Returns: Array of job objects
 */
async function readQueue() {
  try {
    const data = await fs.readFile(QUEUE_FILE, "utf8");
    return JSON.parse(data);
  } catch (error) {
    console.error(`❌ Failed to read queue.json:`, error.message);
    return [];
  }
}

/**
 * Write to queue.json atomically
 */
async function writeQueue(queue) {
  try {
    const tempFile = QUEUE_FILE + ".tmp";
    await fs.writeFile(tempFile, JSON.stringify(queue, null, 2));
    await fs.rename(tempFile, QUEUE_FILE);
  } catch (error) {
    console.error(`❌ Failed to write queue.json:`, error.message);
  }
}

/**
 * Append to activity log file
 */
async function logActivity(message) {
  try {
    const timestamp = new Date().toISOString();
    await fs.appendFile(LOGS_FILE, `[${timestamp}] ${message}\n`);
  } catch (error) {
    // Silent fail for logging
  }
}

// ============================================================================
// JOB QUEUE MANAGEMENT
// ============================================================================

/**
 * Job Types:
 * - activatePlan: Activate a plan after payment detection
 * - collectEMI: Collect due EMI payment
 */

/**
 * Add a job to the queue
 * Job structure: { id, type, data, status, createdAt, attempts, lastError }
 */
async function enqueueJob(type, data) {
  const queue = await readQueue();
  
  // Check for duplicate jobs (avoid re-queuing same operation)
  const isDuplicate = queue.some(job => 
    job.type === type && 
    job.status === "pending" &&
    JSON.stringify(job.data) === JSON.stringify(data)
  );
  
  if (isDuplicate) {
    console.log(`⏭️  Job already queued: ${type} ${JSON.stringify(data)}`);
    return null;
  }
  
  const job = {
    id: `${type}_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
    type,
    data,
    status: "pending", // pending | processing | completed | failed
    createdAt: Date.now(),
    attempts: 0,
    lastError: null,
  };
  
  queue.push(job);
  await writeQueue(queue);
  
  console.log(`📥 Job queued: ${job.id} (${type})`);
  await logActivity(`JOB_QUEUED: ${job.id} - ${type} - ${JSON.stringify(data)}`);
  
  return job;
}

/**
 * Get next pending job from queue
 */
async function dequeueJob() {
  const queue = await readQueue();
  const pendingJob = queue.find(job => job.status === "pending");
  
  if (!pendingJob) return null;
  
  // Mark as processing
  pendingJob.status = "processing";
  pendingJob.attempts++;
  pendingJob.processingStartedAt = Date.now();
  
  await writeQueue(queue);
  return pendingJob;
}

/**
 * Mark job as completed and remove from queue
 */
async function completeJob(jobId, result = null) {
  const queue = await readQueue();
  const jobIndex = queue.findIndex(job => job.id === jobId);
  
  if (jobIndex === -1) return;
  
  const job = queue[jobIndex];
  job.status = "completed";
  job.completedAt = Date.now();
  job.result = result;
  
  console.log(`✅ Job completed: ${jobId}`);
  await logActivity(`JOB_COMPLETED: ${jobId} - ${JSON.stringify(result)}`);
  
  // Remove completed jobs after 5 minutes (keep recent history)
  const RETENTION_TIME = 5 * 60 * 1000;
  const updatedQueue = queue.filter(j => 
    j.status !== "completed" || (Date.now() - j.completedAt < RETENTION_TIME)
  );
  
  await writeQueue(updatedQueue);
}

/**
 * Mark job as failed (with retry logic)
 */
async function failJob(jobId, error) {
  const queue = await readQueue();
  const job = queue.find(j => j.id === jobId);
  
  if (!job) return;
  
  job.lastError = error.message || String(error);
  
  const MAX_RETRIES = 3;
  
  // Retry logic for transient errors
  if (job.attempts < MAX_RETRIES && isRetriableError(error)) {
    job.status = "pending"; // Re-queue for retry
    console.log(`♻️  Job retry scheduled: ${jobId} (attempt ${job.attempts}/${MAX_RETRIES})`);
    await logActivity(`JOB_RETRY: ${jobId} - ${job.lastError}`);
  } else {
    job.status = "failed";
    job.failedAt = Date.now();
    console.error(`❌ Job failed permanently: ${jobId} - ${job.lastError}`);
    await logActivity(`JOB_FAILED: ${jobId} - ${job.lastError}`);
  }
  
  await writeQueue(queue);
}

/**
 * Determine if error is transient and should be retried
 */
function isRetriableError(error) {
  const errorMsg = error.message || String(error);
  
  // Transient network errors
  if (errorMsg.includes('ECONNREFUSED') || 
      errorMsg.includes('timeout') || 
      errorMsg.includes('ETIMEDOUT') ||
      errorMsg.includes('network')) {
    return true;
  }
  
  // Non-retriable errors
  if (errorMsg.includes('already known') || 
      errorMsg.includes('nonce too low') ||
      errorMsg.includes('insufficient funds') ||
      errorMsg.includes('Plan not active') ||
      errorMsg.includes('Plan completed')) {
    return false;
  }
  
  return true; // Default: retry
}

// ============================================================================
// EMBEDDED SCHEDULER #1: PAYMENT DETECTION (Every 5 seconds)
// ============================================================================

/**
 * Scans storage.json for inactive monitors
 * Checks blockchain for incoming ETH payments
 * Enqueues activation job when payment detected
 */
async function paymentDetectionScheduler() {
  try {
    const storage = await readStorage();
    const receivers = Object.keys(storage);
    
    if (receivers.length === 0) {
      console.log(`ℹ️  [PAYMENT_DETECTION] No monitors registered`);
      return;
    }
    
    console.log(`🔍 [PAYMENT_DETECTION] Scanning ${receivers.length} monitors for incoming payments...`);
    
    for (const receiver of receivers) {
      const monitor = storage[receiver];
      
      // Skip if already active (payment already received and activated)
      if (monitor.active) continue;
      
      // Detect payment via blockchain event logs
      const sender = await detectPaymentViaEventLogs(
        receiver,
        monitor.contract,
        monitor.chainId,
        monitor.planId
      );
      
      if (sender) {
        console.log(`\n💰 [PAYMENT_DETECTION] Payment detected!`);
        console.log(`   Plan #${monitor.planId}: ${sender} → ${receiver}\n`);
        
        // Update storage with sender info
        monitor.sender = sender;
        monitor.paymentDetectedAt = Date.now();
        await writeStorage(storage);
        
        // Enqueue activation job
        await enqueueJob("activatePlan", {
          planId: monitor.planId,
          receiver,
          sender,
          contract: monitor.contract,
          chainId: monitor.chainId,
        });
      }
    }
  } catch (error) {
    console.error(`❌ [PAYMENT_DETECTION] Scheduler error:`, error.message);
  }
}

/**
 * Query blockchain for incoming ETH transfers to receiver address
 * Uses RPC direct transaction scanning for fastest detection
 */
async function detectPaymentViaEventLogs(receiver, contract, chainId, planId) {
  try {
    const rpcUrl = RPC_ENDPOINTS[chainId];
    if (!rpcUrl) return null;

    const provider = new ethers.providers.JsonRpcProvider(rpcUrl);
    const currentBlock = await provider.getBlockNumber();
    const fromBlock = Math.max(0, currentBlock - 50); // Scan last 50 blocks

    console.log(`   🔍 Scanning blocks ${fromBlock}-${currentBlock} for Plan #${planId}`);

    // Scan recent blocks for ETH transfers to receiver
    for (let blockNum = currentBlock; blockNum >= fromBlock; blockNum--) {
      try {
        const block = await provider.getBlock(blockNum);
        if (!block || !block.transactions || block.transactions.length === 0) continue;

        for (const txHash of block.transactions) {
          try {
            const tx = await provider.getTransaction(txHash);
            if (!tx) continue;

            // Check if ETH sent to receiver
            if (tx.to && 
                tx.to.toLowerCase() === receiver.toLowerCase() && 
                tx.value && 
                tx.value.gt(0)) {
              
              const receipt = await provider.getTransactionReceipt(txHash);
              
              if (receipt && receipt.status === 1) {
                console.log(`\n✅ INCOMING ETH DETECTED & VERIFIED!`);
                console.log(`   Sender: ${tx.from}`);
                console.log(`   Amount: ${ethers.utils.formatEther(tx.value)} ETH`);
                console.log(`   Tx: ${tx.hash}\n`);
                
                await logActivity(`PAYMENT_DETECTED: Plan #${planId} - ${tx.from} → ${receiver} - ${ethers.utils.formatEther(tx.value)} ETH`);
                
                return tx.from;
              }
            }
          } catch (txError) {
            // Continue scanning
          }
        }
      } catch (blockError) {
        // Continue scanning
      }
    }

    return null;
  } catch (error) {
    console.error(`❌ [PAYMENT_DETECTION] Error:`, error.message);
    return null;
  }
}

// ============================================================================
// EMBEDDED SCHEDULER #2: EMI COLLECTION (Every 30 seconds)
// ============================================================================

/**
 * Scans storage.json for active plans
 * Checks on-chain if EMI payment is due
 * Enqueues collection job when payment is due
 */
async function emiCollectionScheduler() {
  try {
    const storage = await readStorage();
    const receivers = Object.keys(storage);
    
    const activePlans = receivers.filter(r => storage[r].active);
    
    if (activePlans.length === 0) {
      console.log(`ℹ️  [EMI_COLLECTION] No active plans to check`);
      return;
    }
    
    console.log(`💸 [EMI_COLLECTION] Checking ${activePlans.length} active plans for due EMI...`);
    
    let dueCount = 0;
    
    for (const receiver of activePlans) {
      const monitor = storage[receiver];
      
      // Check if payment is due on-chain
      const dueInfo = await isPaymentDue(
        monitor.contract,
        monitor.chainId,
        monitor.planId
      );
      
      if (dueInfo) {
        dueCount++;
        console.log(`   💰 Plan #${monitor.planId} DUE - ${ethers.utils.formatEther(dueInfo.emi)} ETH`);
        
        // Enqueue collection job
        await enqueueJob("collectEMI", {
          planId: monitor.planId,
          receiver,
          contract: monitor.contract,
          chainId: monitor.chainId,
          emi: dueInfo.emi.toString(),
        });
      }
    }
    
    if (dueCount === 0) {
      console.log(`   ✅ All plans up to date`);
    } else {
      console.log(`   📥 ${dueCount} EMI collections queued`);
    }
  } catch (error) {
    console.error(`❌ [EMI_COLLECTION] Scheduler error:`, error.message);
  }
}

/**
 * Check if EMI payment is due for a specific plan
 * Queries contract directly via RPC
 */
async function isPaymentDue(contractAddress, chainId, planId) {
  try {
    const rpcUrl = RPC_ENDPOINTS[chainId];
    if (!rpcUrl) return null;

    const provider = new ethers.providers.JsonRpcProvider(rpcUrl);
    const contractInstance = new ethers.Contract(
      contractAddress,
      CONTRACT_ABI_FULL,
      provider
    );

    const plan = await contractInstance.plans(planId);
    
    // Unpack plan struct (9 fields)
    const [sender, receiver, emi, interval, total, paid, nextPay, active, paymentReceived] = plan;
    
    const currentTime = Math.floor(Date.now() / 1000);
    
    // Check if payment is due
    if (active && 
        paid.lt(total) && 
        currentTime >= nextPay.toNumber() && 
        paymentReceived) {
      
      return {
        planId,
        sender,
        receiver,
        emi,
        nextPay: nextPay.toNumber(),
        total,
        paid,
      };
    }
    
    return null;
  } catch (error) {
    console.error(`❌ [isPaymentDue] Error for Plan #${planId}:`, error.message);
    return null;
  }
}

// ============================================================================
// JOB QUEUE WORKER (Every 2 seconds)
// ============================================================================

/**
 * Processes pending jobs from queue.json
 * Handles two job types: activatePlan and collectEMI
 */
async function queueWorker() {
  try {
    const job = await dequeueJob();
    
    if (!job) {
      // No pending jobs
      return;
    }
    
    console.log(`\n⚙️  [WORKER] Processing job: ${job.id} (${job.type})`);
    await logActivity(`JOB_PROCESSING: ${job.id} - ${job.type}`);
    
    try {
      if (job.type === "activatePlan") {
        await processActivatePlanJob(job);
      } else if (job.type === "collectEMI") {
        await processCollectEMIJob(job);
      } else {
        throw new Error(`Unknown job type: ${job.type}`);
      }
      
      await completeJob(job.id, { success: true });
    } catch (error) {
      console.error(`❌ [WORKER] Job failed: ${job.id}`, error.message);
      await failJob(job.id, error);
    }
  } catch (error) {
    console.error(`❌ [WORKER] Critical error:`, error.message);
  }
}

/**
 * Process activatePlan job
 * Calls contract.activatePlanRaw() with admin wallet
 */
async function processActivatePlanJob(job) {
  const { planId, receiver, sender, contract, chainId } = job.data;
  
  console.log(`🔓 [WORKER] Activating Plan #${planId}...`);
  console.log(`   Sender: ${sender}, Receiver: ${receiver}`);
  
  const adminPrivateKey = process.env.ADMIN_PRIVATE_KEY;
  if (!adminPrivateKey) {
    throw new Error("ADMIN_PRIVATE_KEY not configured");
  }
  
  const rpcUrl = RPC_ENDPOINTS[chainId];
  if (!rpcUrl) {
    throw new Error(`No RPC for chain ${chainId}`);
  }
  
  const provider = new ethers.providers.JsonRpcProvider(rpcUrl);
  const adminSigner = new ethers.Wallet(adminPrivateKey, provider);
  
  // Check admin wallet balance
  const balance = await provider.getBalance(adminSigner.address);
  const gasEstimate = ethers.utils.parseEther("0.02");
  
  if (balance.lt(gasEstimate)) {
    throw new Error(`Insufficient admin balance: ${ethers.utils.formatEther(balance)} ETH`);
  }
  
  const emiContract = new ethers.Contract(contract, CONTRACT_ABI_FULL, adminSigner);
  
  // Get nonce to prevent "already known" errors
  const nonce = await provider.getTransactionCount(adminSigner.address, "pending");
  
  console.log(`   📤 Calling activatePlanRaw(${planId}, ${sender})...`);
  
  const tx = await emiContract.activatePlanRaw(planId, sender, {
    gasLimit: 500000,
    nonce,
  });
  
  console.log(`   ⏳ Tx sent: ${tx.hash}`);
  
  // Update storage immediately (optimistic update)
  const storage = await readStorage();
  if (storage[receiver]) {
    storage[receiver].active = true;
    storage[receiver].activationTx = tx.hash;
    storage[receiver].activatedAt = Date.now();
    await writeStorage(storage);
  }
  
  // Wait for confirmation (blocking)
  const receipt = await tx.wait();
  
  console.log(`   ✅ Plan #${planId} ACTIVATED! Block: ${receipt.blockNumber}`);
  await logActivity(`PLAN_ACTIVATED: Plan #${planId} - Tx: ${receipt.transactionHash}`);
  
  return { txHash: receipt.transactionHash, blockNumber: receipt.blockNumber };
}

/**
 * Process collectEMI job
 * Calls contract.collectPayment() with admin wallet
 */
async function processCollectEMIJob(job) {
  const { planId, receiver, contract, chainId, emi } = job.data;
  
  console.log(`💸 [WORKER] Collecting EMI for Plan #${planId}...`);
  console.log(`   Amount: ${ethers.utils.formatEther(emi)} ETH`);
  
  const adminPrivateKey = process.env.ADMIN_PRIVATE_KEY;
  if (!adminPrivateKey) {
    throw new Error("ADMIN_PRIVATE_KEY not configured");
  }
  
  const rpcUrl = RPC_ENDPOINTS[chainId];
  const provider = new ethers.providers.JsonRpcProvider(rpcUrl);
  const adminSigner = new ethers.Wallet(adminPrivateKey, provider);
  
  // Check admin balance
  const balance = await provider.getBalance(adminSigner.address);
  const gasEstimate = ethers.utils.parseEther("0.02");
  
  if (balance.lt(gasEstimate)) {
    throw new Error(`Insufficient admin balance: ${ethers.utils.formatEther(balance)} ETH`);
  }
  
  const emiContract = new ethers.Contract(contract, CONTRACT_ABI_FULL, adminSigner);
  
  console.log(`   📤 Calling collectPayment(${planId})...`);
  
  const tx = await emiContract.collectPayment(planId, { gasLimit: 500000 });
  
  console.log(`   ⏳ Tx sent: ${tx.hash}`);
  
  const receipt = await tx.wait();
  
  console.log(`   ✅ EMI collected! Tx: ${receipt.transactionHash}`);
  await logActivity(`EMI_COLLECTED: Plan #${planId} - ${ethers.utils.formatEther(emi)} ETH - Tx: ${receipt.transactionHash}`);
  
  return { txHash: receipt.transactionHash, amount: emi };
}

// ============================================================================
// SCHEDULER INITIALIZATION
// ============================================================================

let schedulersActive = false;

/**
 * Start all embedded schedulers
 * Runs independently in the background
 */
function startSchedulers() {
  if (schedulersActive) return;
  
  console.log(`\n🔥 [SCHEDULERS] Starting embedded schedulers...\n`);
  
  // Scheduler 1: Payment Detection (every 5 seconds)
  setInterval(() => {
    paymentDetectionScheduler().catch(err => {
      console.error(`❌ Payment detection scheduler crashed:`, err);
    });
  }, PAYMENT_DETECTION_INTERVAL);
  
  console.log(`✅ Payment Detection Scheduler: Running (every ${PAYMENT_DETECTION_INTERVAL/1000}s)`);
  
  // Scheduler 2: EMI Collection (every 30 seconds)
  setInterval(() => {
    emiCollectionScheduler().catch(err => {
      console.error(`❌ EMI collection scheduler crashed:`, err);
    });
  }, EMI_COLLECTION_INTERVAL);
  
  console.log(`✅ EMI Collection Scheduler: Running (every ${EMI_COLLECTION_INTERVAL/1000}s)`);
  
  // Worker: Job Queue Processor (every 2 seconds)
  setInterval(() => {
    queueWorker().catch(err => {
      console.error(`❌ Queue worker crashed:`, err);
    });
  }, QUEUE_PROCESSING_INTERVAL);
  
  console.log(`✅ Job Queue Worker: Running (every ${QUEUE_PROCESSING_INTERVAL/1000}s)\n`);
  
  schedulersActive = true;
}

// ============================================================================
// REST API ENDPOINTS
// ============================================================================

/**
 * PHASE 2: Monitor Registration
 * POST /monitor
 * Body: { planId, receiver, chainId, contract }
 */
app.post("/monitor", async (req, res) => {
  const { planId, receiver, chainId, contract } = req.body;

  console.log(`\n🚀 [MONITOR REGISTRATION] Received`);
  console.log(`   Plan ID: #${planId}`);
  console.log(`   Receiver: ${receiver}`);
  console.log(`   Chain ID: ${chainId}`);
  console.log(`   Contract: ${contract}\n`);

  if (!planId || !receiver || !chainId || !contract) {
    return res.status(400).json({
      success: false,
      error: "Missing required fields: planId, receiver, chainId, contract",
    });
  }

  // Read current storage
  const storage = await readStorage();
  
  // Store monitor
  storage[receiver.toLowerCase()] = {
    planId: parseInt(planId),
    receiver: receiver.toLowerCase(),
    chainId: parseInt(chainId),
    contract: contract.toLowerCase(),
    active: false,
    sender: null,
    createdAt: Date.now(),
  };
  
  await writeStorage(storage);
  await logActivity(`MONITOR_REGISTERED: Plan #${planId} - Receiver: ${receiver}`);

  console.log(`✅ Monitor registered in storage.json`);
  console.log(`   Total monitors: ${Object.keys(storage).length}\n`);

  res.json({
    success: true,
    message: `✅ Monitoring activated for Plan #${planId}`,
    receiver: receiver.toLowerCase(),
    planId: parseInt(planId),
    statusUrl: `https://${req.headers.host}/status/${receiver.toLowerCase()}`,
    schedulerStatus: "✅ EMBEDDED SCHEDULERS ACTIVE",
  });
});

/**
 * Get monitor status
 * GET /status/:receiver
 */
app.get("/status/:receiver", async (req, res) => {
  const receiver = req.params.receiver.toLowerCase();
  const storage = await readStorage();
  const monitor = storage[receiver];

  if (!monitor) {
    return res.json({
      receiver,
      active: false,
      schedulersActive,
      message: "No active monitoring",
    });
  }

  res.json({
    receiver,
    active: monitor.active,
    planId: monitor.planId,
    schedulersActive,
    status: monitor.active ? "✅ ACTIVE" : "⏳ WAITING FOR PAYMENT",
    sender: monitor.sender || null,
    message: monitor.active
      ? `Plan #${monitor.planId} ACTIVE. Schedulers collecting EMI automatically!`
      : `Plan #${monitor.planId} awaiting activation payment...`,
    createdAt: monitor.createdAt,
    activatedAt: monitor.activatedAt || null,
  });
});

/**
 * Debug: Check plan due status
 * GET /debug/plan/:planId/:contract
 */
app.get("/debug/plan/:planId/:contract", async (req, res) => {
  const { planId, contract } = req.params;
  const chainId = 11155111; // Sepolia

  const dueInfo = await isPaymentDue(contract, chainId, planId);

  res.json({
    planId,
    contract,
    dueInfo,
    isDue: !!dueInfo,
    timestamp: Math.floor(Date.now() / 1000),
  });
});

/**
 * Get all monitors (admin endpoint)
 * GET /admin/monitors
 */
app.get("/admin/monitors", async (req, res) => {
  const storage = await readStorage();
  const queue = await readQueue();
  
  res.json({
    count: Object.keys(storage).length,
    schedulersActive,
    queueSize: queue.length,
    pendingJobs: queue.filter(j => j.status === "pending").length,
    processingJobs: queue.filter(j => j.status === "processing").length,
    storage,
    queue,
  });
});

/**
 * Get job queue status
 * GET /admin/queue
 */
app.get("/admin/queue", async (req, res) => {
  const queue = await readQueue();
  
  res.json({
    total: queue.length,
    pending: queue.filter(j => j.status === "pending").length,
    processing: queue.filter(j => j.status === "processing").length,
    completed: queue.filter(j => j.status === "completed").length,
    failed: queue.filter(j => j.status === "failed").length,
    jobs: queue,
  });
});

/**
 * Get activity logs
 * GET /admin/logs
 */
app.get("/admin/logs", async (req, res) => {
  try {
    const logs = await fs.readFile(LOGS_FILE, "utf8");
    const lines = logs.split("\n").filter(l => l.trim());
    
    res.json({
      count: lines.length,
      logs: lines.slice(-100), // Last 100 entries
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

/**
 * Clear all monitors (testing)
 * POST /admin/clear
 */
app.post("/admin/clear", async (req, res) => {
  const storage = await readStorage();
  const count = Object.keys(storage).length;
  
  await writeStorage({});
  await writeQueue([]);
  
  res.json({
    success: true,
    message: `Cleared ${count} monitors and reset queue`,
  });
});

/**
 * Manually trigger payment detection (testing)
 * POST /admin/trigger-detection
 */
app.post("/admin/trigger-detection", async (req, res) => {
  await paymentDetectionScheduler();
  res.json({ success: true, message: "Payment detection triggered" });
});

/**
 * Manually trigger EMI collection (testing)
 * POST /admin/trigger-collection
 */
app.post("/admin/trigger-collection", async (req, res) => {
  await emiCollectionScheduler();
  res.json({ success: true, message: "EMI collection check triggered" });
});

// ============================================================================
// ERROR HANDLING
// ============================================================================

app.use((err, req, res, next) => {
  console.error("Server error:", err);
  res.status(500).json({
    success: false,
    error: err.message || "Internal server error",
  });
});

// ============================================================================
// SERVER INITIALIZATION
// ============================================================================

async function startServer() {
  // Initialize storage files
  await initializeStorage();
  
  // Start embedded schedulers
  startSchedulers();
  
  // Start Express server
  const PORT = process.env.PORT || 3000;
  app.listen(PORT, () => {
    console.log(`\n${"=".repeat(70)}`);
    console.log(`🚀 SERVER RUNNING ON PORT ${PORT}`);
    console.log(`${"=".repeat(70)}\n`);
  });
}

// Start everything
startServer().catch(err => {
  console.error("Failed to start server:", err);
  process.exit(1);
});

module.exports = app;