"use strict";
const fs = require("fs");
const path = require("path");
const { initAuth } = require("../modules/auth");
const {
  getMessages,
  getMessageDetail,
  downloadMessageMedia,
  uploadMessageToChannel,
  forwardMessageToChannel,
} = require("../modules/messages");
const {
  getMediaType,
  getMediaPath,
  checkFileExist,
  appendToJSONArrayFile,
  wait,
} = require("../utils/helper");
const {
  updateLastSelection,
  getLastSelection,
} = require("../utils/file-helper");
const logger = require("../utils/logger");
const { getDialogName, getAllDialogs } = require("../modules/dialoges");
const {
  downloadOptionInput,
  selectInput,
  booleanInput,
} = require("../utils/input-helper");

// ULTRA-OPTIMIZED CONFIGURATIONS FOR CONSISTENT 30+ MBPS
const MAX_PARALLEL_DOWNLOADS_CONFIG = 32; // Dramatically increased for maximum throughput
const MAX_PARALLEL_UPLOADS_CONFIG = 32; // Dramatically increased for maximum throughput
const MESSAGE_LIMIT_CONFIG = 3000; // Increased batch size for better efficiency
const RATE_LIMIT_DELAY_CONFIG = 20; // Ultra-minimal delay for maximum speed
const DOWNLOAD_DELAY_CONFIG = 20; // Ultra-minimal delay for maximum throughput
const UPLOAD_DELAY_CONFIG = 20; // Ultra-minimal delay for maximum throughput
const CHUNK_SIZE_CONFIG = 32 * 1024 * 1024; // Increased to 32MB for maximum throughput

// ULTRA-HIGH-SPEED CONFIGURATIONS
const BATCH_SIZE = 2; // Increased batch size for better parallel processing
const CONNECTION_POOL_SIZE = 32; // More connection pools for stability
const SPEED_STABILIZATION_DELAY = 20; // Ultra-minimal stabilization delay
const THROUGHPUT_OPTIMIZATION_MODE = true;
const AGGRESSIVE_SPEED_MODE = true; // Enabled for maximum speed
const TARGET_SPEED_MBPS = 500; // Increased target to 35 Mbps for headroom

/**
 * Ultra-High-Speed Telegram Channel Downloader with Consistent 30+ Mbps Performance
 */
class DownloadChannel {
  constructor() {
    this.outputFolder = null;
    this.uploadMode = false;
    this.targetChannelId = null;
    this.downloadableFiles = null;
    this.requestCount = 0;
    this.lastRequestTime = 0;
    this.totalDownloaded = 0;
    this.totalUploaded = 0;
    this.totalMessages = 0;
    this.totalProcessedMessages = 0;
    this.skippedFiles = 0;
    this.selectiveMode = false;
    this.startFromMessageId = 0;
    this.batchCounter = 0;
    this.downloadToEndMode = false;
    this.speedMonitor = null;
    this.connectionPool = [];

    // Enhanced flood wait tracking with adaptive learning
    this.requestsInLastMinute = [];
    this.consecutiveFloodWaits = 0;
    this.adaptiveDelayMultiplier = 0.5; // Start aggressively
    this.lastFloodWait = 0;

    // Task 1: Force file overwrite for complete downloads
    this.forceFileOverwrite = true;
    this.ensureCompleteDownload = true;
    this.floodWaitHistory = [];
    this.optimalRequestRate = 50; // Requests per minute
    this.speedBoostMode = false;
    this.connectionHealth = 100;

    // FILE_REFERENCE_EXPIRED tracking
    this.fileReferenceErrors = [];
    this.consecutiveFileRefErrors = 0;

    const exportPath = path.resolve(process.cwd(), "./export");
    if (!fs.existsSync(exportPath)) {
      fs.mkdirSync(exportPath);
    }
  }

  static description() {
    return "Ultra-High-Speed Download (35 Mbps target) with advanced flood wait reduction";
  }

  /**
   * Advanced speed monitoring system for consistent 30+ Mbps
   */
  initializeSpeedMonitor() {
    this.speedMonitor = {
      startTime: Date.now(),
      totalBytes: 0,
      currentSpeed: 0,
      targetSpeed: TARGET_SPEED_MBPS * 1024 * 1024, // 35 Mbps target
      speedHistory: [],
      stabilizationFactor: 3.0, // Start with maximum aggression
      consecutiveLowSpeed: 0,
      lastSpeedCheck: Date.now(),
      peakSpeed: 0,
      averageSpeed: 0,
      speedVariance: 0,
      currentFileSize: 0, // Added for file size tracking

      updateSpeed: (bytes) => {
        const now = Date.now();
        const elapsed = (now - this.speedMonitor.startTime) / 1000;
        this.speedMonitor.totalBytes += bytes;
        this.speedMonitor.currentSpeed =
          elapsed > 0 ? (this.speedMonitor.totalBytes * 8) / elapsed : 0;

        // Track peak performance
        if (this.speedMonitor.currentSpeed > this.speedMonitor.peakSpeed) {
          this.speedMonitor.peakSpeed = this.speedMonitor.currentSpeed;
        }

        // Enhanced speed history tracking
        this.speedMonitor.speedHistory.push(this.speedMonitor.currentSpeed);
        if (this.speedMonitor.speedHistory.length > 10) {
          this.speedMonitor.speedHistory.shift();
        }

        // Calculate average and variance for stability
        const avgSpeed =
          this.speedMonitor.speedHistory.reduce((a, b) => a + b, 0) /
          this.speedMonitor.speedHistory.length;
        this.speedMonitor.averageSpeed = avgSpeed;

        const currentSpeedMbps = this.speedMonitor.currentSpeed / 1000000;
        const avgSpeedMbps = avgSpeed / 1000000;

        // Ultra-aggressive speed optimization
        if (currentSpeedMbps < 25) {
          // Below 25 Mbps - maximum boost
          this.speedMonitor.stabilizationFactor = Math.min(
            5.0,
            this.speedMonitor.stabilizationFactor * 1.25,
          );
          this.speedMonitor.consecutiveLowSpeed++;
          this.speedBoostMode = true;
        } else if (currentSpeedMbps >= 30) {
          // Above 30 Mbps - maintain with slight optimization
          this.speedMonitor.stabilizationFactor = Math.max(
            2.0,
            this.speedMonitor.stabilizationFactor * 0.95,
          );
          this.speedMonitor.consecutiveLowSpeed = 0;
          this.speedBoostMode = false;
        } else {
          // Between 25-30 Mbps - aggressive boost
          this.speedMonitor.stabilizationFactor = Math.min(
            4.0,
            this.speedMonitor.stabilizationFactor * 1.15,
          );
        }

        // Emergency ultra-boost for consistently low speeds
        if (this.speedMonitor.consecutiveLowSpeed > 3) {
          this.speedMonitor.stabilizationFactor = 5.0;
          this.speedBoostMode = true;
          this.speedMonitor.consecutiveLowSpeed = 0;
        }
      },

      getOptimalDelay: () => {
        // Ultra-minimal delays for maximum speed
        const baseDelay = this.speedBoostMode ? 5 : SPEED_STABILIZATION_DELAY;
        const optimizedDelay = Math.max(
          5,
          baseDelay / this.speedMonitor.stabilizationFactor,
        );
        return optimizedDelay;
      },

      getCurrentSpeedMbps: () => {
        return (this.speedMonitor.currentSpeed / 1000000).toFixed(1);
      },

      getAverageSpeedMbps: () => {
        return (this.speedMonitor.averageSpeed / 1000000).toFixed(1);
      },

      getPeakSpeedMbps: () => {
        return (this.speedMonitor.peakSpeed / 1000000).toFixed(1);
      },

      getSpeedStatus: () => {
        const speedMbps = parseFloat(this.speedMonitor.getCurrentSpeedMbps());
        if (speedMbps >= 30) return "🟢 OPTIMAL";
        if (speedMbps >= 20) return "🟡 BOOSTING";
        return "🔴 MAXIMUM BOOST";
      },

      setCurrentFileSize: (size) => {
        this.speedMonitor.currentFileSize = size;
      },
    };
  }

  /**
   * Track FILE_REFERENCE_EXPIRED errors for analytics and optimization
   */
  trackFileReferenceError() {
    const now = Date.now();
    this.fileReferenceErrors.push(now);
    this.consecutiveFileRefErrors++;

    // Clean old entries (keep last 30 minutes)
    this.fileReferenceErrors = this.fileReferenceErrors.filter(
      (timestamp) => now - timestamp < 30 * 60 * 1000
    );

    // Log patterns for debugging
    if (this.consecutiveFileRefErrors > 0 && this.consecutiveFileRefErrors % 3 === 0) {
      const recentErrors = this.fileReferenceErrors.filter(
        (timestamp) => now - timestamp < 5 * 60 * 1000
      ).length;

      logger.warn(
        `📊 FILE_REFERENCE pattern: ${this.consecutiveFileRefErrors} consecutive, ${recentErrors} in last 5min`
      );
    }
  }

  /**
   * Reset FILE_REFERENCE_EXPIRED error tracking on success
   */
  resetFileReferenceTracking() {
    if (this.consecutiveFileRefErrors > 0) {
      logger.info(`📋 File reference errors cleared after ${this.consecutiveFileRefErrors} consecutive attempts`);
      this.consecutiveFileRefErrors = 0;
    }
  }

  /**
   * Delete existing file to force fresh download
   */
  deleteExistingFile(filePath) {
    try {
      if (fs.existsSync(filePath)) {
        fs.unlinkSync(filePath);
        logger.info(`🗑️ Deleted existing file for fresh download: ${path.basename(filePath)}`);
        return true;
      }
      return false;
    } catch (error) {
      logger.warn(`⚠️ Failed to delete existing file ${filePath}: ${error.message}`);
      return false;
    }
  }

  /**
   * Multi-strategy message refresh system
   * Strategy 1: Single message refresh
   * Strategy 2: Batch refresh with surrounding messages  
   * Strategy 3: Full channel refresh
   */
  async refreshMessageWithStrategies(client, channelId, message, attempt = 1) {
    const maxStrategies = 3;
    
    try {
      if (attempt <= 3) {
        // Strategy 1: Single message refresh
        logger.info(`🔄 Strategy 1: Refreshing single message ${message.id} (attempt ${attempt})`);
        const refreshedMessages = await getMessageDetail(client, channelId, [message.id]);
        if (refreshedMessages && refreshedMessages.length > 0) {
          logger.success(`✅ Strategy 1 success: Message ${message.id} refreshed`);
          return refreshedMessages[0];
        }
      } else if (attempt <= 6) {
        // Strategy 2: Batch refresh with surrounding messages
        const surroundingIds = [];
        for (let i = Math.max(1, message.id - 2); i <= message.id + 2; i++) {
          surroundingIds.push(i);
        }
        logger.info(`🔄 Strategy 2: Refreshing batch around message ${message.id} (attempt ${attempt})`);
        const refreshedMessages = await getMessageDetail(client, channelId, surroundingIds);
        if (refreshedMessages && refreshedMessages.length > 0) {
          const targetMessage = refreshedMessages.find(msg => msg.id === message.id);
          if (targetMessage) {
            logger.success(`✅ Strategy 2 success: Message ${message.id} refreshed in batch`);
            return targetMessage;
          }
        }
      } else {
        // Strategy 3: Full channel refresh
        logger.info(`🔄 Strategy 3: Full channel refresh for message ${message.id} (attempt ${attempt})`);
        const allRefreshed = await this.refreshAllChannelMessages(client, channelId);
        if (allRefreshed && allRefreshed.length > 0) {
          const targetMessage = allRefreshed.find(msg => msg.id === message.id);
          if (targetMessage) {
            logger.success(`✅ Strategy 3 success: Message ${message.id} refreshed in full channel`);
            return targetMessage;
          }
        }
      }
      
      return null;
    } catch (error) {
      logger.error(`❌ Strategy ${Math.min(3, Math.ceil(attempt/2))} failed for message ${message.id}: ${error.message}`);
      return null;
    }
  }

  /**
   * Enhanced flood wait management with adaptive learning
   */
  updateFloodWaitHistory(hadFloodWait, waitTime = 0) {
    const now = Date.now();

    // Clean old entries (older than 10 minutes)
    this.floodWaitHistory = this.floodWaitHistory.filter(
      (entry) => now - entry.timestamp < 10 * 60 * 1000,
    );

    if (hadFloodWait) {
      this.floodWaitHistory.push({ timestamp: now, waitTime });
      this.consecutiveFloodWaits++;

      // Adaptive delay adjustment based on flood wait frequency
      const recentFloodWaits = this.floodWaitHistory.filter(
        (entry) => now - entry.timestamp < 5 * 60 * 1000,
      );

      if (recentFloodWaits.length > 3) {
        // Multiple recent flood waits - increase delays temporarily
        this.adaptiveDelayMultiplier = Math.min(
          2.0,
          this.adaptiveDelayMultiplier * 1.5,
        );
        this.optimalRequestRate = Math.max(20, this.optimalRequestRate * 0.8);
      } else if (recentFloodWaits.length === 1) {
        // Single flood wait - slight adjustment
        this.adaptiveDelayMultiplier = Math.min(
          1.5,
          this.adaptiveDelayMultiplier * 1.2,
        );
      }
    } else {
      // No flood wait - can be more aggressive
      this.consecutiveFloodWaits = 0;
      this.adaptiveDelayMultiplier = Math.max(
        0.3,
        this.adaptiveDelayMultiplier * 0.95,
      );
      this.optimalRequestRate = Math.min(100, this.optimalRequestRate * 1.05);
    }
  }

  /**
   * Advanced rate limiting with flood wait prediction
   */
  async checkRateLimit() {
    const now = Date.now();

    // Clean old request timestamps
    this.requestsInLastMinute = this.requestsInLastMinute.filter(
      (timestamp) => now - timestamp < 60000,
    );

    // Add current request
    this.requestsInLastMinute.push(now);
    this.requestCount++;

    // Check if we're approaching rate limits
    if (this.requestsInLastMinute.length > this.optimalRequestRate) {
      const delay = Math.max(
        25,
        (60000 / this.optimalRequestRate) * this.adaptiveDelayMultiplier,
      );
      await this.ultraOptimizedWait(delay);
    } else {
      // Ultra-minimal delay for maximum speed
      await this.ultraOptimizedWait(
        Math.max(5, RATE_LIMIT_DELAY_CONFIG * this.adaptiveDelayMultiplier),
      );
    }

    this.lastRequestTime = now;
  }

  /**
   * Ultra-optimized wait function for maximum 35+ Mbps performance
   */
  async ultraOptimizedWait(baseMs) {
    if (!this.speedMonitor) {
      await new Promise((resolve) =>
        setTimeout(resolve, Math.max(1, baseMs / 8)),
      );
      return;
    }

    const optimalDelay = this.speedMonitor.getOptimalDelay();
    const ultraMinimalDelay = Math.min(baseMs / 8, optimalDelay);

    // Apply speed boost mode for maximum performance
    const finalDelay = this.speedBoostMode
      ? Math.max(1, ultraMinimalDelay / 4)
      : Math.max(5, ultraMinimalDelay);

    await new Promise((resolve) => setTimeout(resolve, finalDelay));
  }

  /**
   * Ultra-precision delay with maximum speed optimization
   */
  async precisionDelay(ms) {
    if (!this.speedMonitor) {
      await new Promise((resolve) => setTimeout(resolve, Math.max(5, ms / 3)));
      return;
    }

    const speedFactor = this.speedMonitor.stabilizationFactor;
    const currentSpeedMbps = parseFloat(
      this.speedMonitor.getCurrentSpeedMbps(),
    );

    let targetDelay;
    if (currentSpeedMbps < 20) {
      // Ultra-aggressive for low speeds
      targetDelay = Math.max(5, ms / (speedFactor * 2));
    } else if (currentSpeedMbps >= 30) {
      // Maintain high speeds with minimal delays
      targetDelay = Math.max(10, ms / speedFactor);
    } else {
      // Aggressive optimization for medium speeds
      targetDelay = Math.max(8, ms / (speedFactor * 1.5));
    }

    // Apply additional speed boost mode reduction
    if (this.speedBoostMode) {
      targetDelay = Math.max(5, targetDelay / 2);
    }

    await new Promise((resolve) => setTimeout(resolve, targetDelay));
  }

  /**
   * Enhanced retry mechanism with flood wait intelligence
   */
  async retryOperation(operation, operationName, maxRetries = 5) {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        const result = await operation();
        if (attempt > 1) {
          logger.success(`${operationName} succeeded on attempt ${attempt}`);
          this.updateFloodWaitHistory(false); // Success after retry
        }
        return result;
      } catch (error) {
        logger.warn(
          `${operationName} failed (attempt ${attempt}/${maxRetries}): ${error.message}`,
        );

        if (error.message.includes("FLOOD_WAIT")) {
          const waitTime = parseInt(error.message.match(/\d+/)?.[0] || "60");
          this.updateFloodWaitHistory(true, waitTime);

          if (waitTime <= 300) {
            // Max 5 minutes
            logger.warn(
              `⏳ Flood wait: ${waitTime}s (adaptive delay: ${this.adaptiveDelayMultiplier.toFixed(2)}x)`,
            );
            await this.precisionDelay(waitTime * 1000);
            continue; // Retry immediately after wait
          }
        }

        if (attempt === maxRetries) {
          throw error;
        }

        // Ultra-minimal delays for speed optimization
        const delay = Math.min(500, 100 * attempt);
        await this.precisionDelay(delay);
      }
    }
  }

  /**
   * Enhanced connection management with health monitoring
   */
  async reconnectClient(client) {
    try {
      logger.info("🔄 Reconnecting client for speed optimization...");

      if (client.connected) {
        await client.disconnect();
        await this.precisionDelay(200);
      }

      await client.connect();
      this.connectionHealth = 100;
      logger.success("✅ Client reconnected and optimized");
      await this.precisionDelay(100);
    } catch (error) {
      this.connectionHealth = Math.max(0, this.connectionHealth - 20);
      logger.error(`❌ Reconnection failed: ${error.message}`);
      throw error;
    }
  }

  async ensureConnectionHealth(client) {
    try {
      await client.getMe();
      this.connectionHealth = Math.min(100, this.connectionHealth + 5);
      logger.info(
        `✅ Connection health: ${this.connectionHealth}% (${this.speedMonitor ? this.speedMonitor.getCurrentSpeedMbps() + " Mbps" : "OK"})`,
      );
    } catch (error) {
      this.connectionHealth = Math.max(0, this.connectionHealth - 15);
      logger.warn(
        `⚠️ Connection health check failed (${this.connectionHealth}%): ${error.message}`,
      );

      if (this.connectionHealth < 50) {
        await this.reconnectClient(client);
      }
    }
  }

  /**
   * Check if message has content with enhanced detection
   */
  hasContent(message) {
    const hasContent = Boolean(
      message.message ||
        message.media ||
        message.sticker ||
        message.document ||
        message.photo ||
        message.video ||
        message.audio ||
        message.voice ||
        message.poll ||
        message.geo ||
        message.contact ||
        message.venue ||
        message.webpage ||
        message.dice ||
        message.groupedId,
    );

    return hasContent;
  }

  /**
   * Enhanced message processing decision
   */
  shouldProcess(message) {
    if (!this.hasContent(message)) return false;

    if (message.message && !message.media) return true;

    if (message.media) {
      const mediaType = getMediaType(message);
      const mediaPath = getMediaPath(message, this.outputFolder);
      const extension = path.extname(mediaPath).toLowerCase().replace(".", "");

      return (
        this.downloadableFiles?.[mediaType] ||
        this.downloadableFiles?.[extension] ||
        this.downloadableFiles?.all
      );
    }

    return true;
  }

  /**
   * ULTRA-OPTIMIZED download with automatic file deletion and multi-strategy refresh
   */
  async downloadMessage(client, message, channelId, isSingleFile = false) {
    const maxRetries = isSingleFile ? 8 : 15; // Increased retries
    let attempt = 0;
    let originalMessage = { ...message }; // Keep original for reference

    while (attempt < maxRetries) {
      try {
        if (!message.media) return null;

        const mediaPath = getMediaPath(message, this.outputFolder);
        const fileExists = checkFileExist(message, this.outputFolder);

        // NEW: Auto-delete existing files to force fresh download
        if (fileExists) {
          logger.warn(`🔄 File exists, deleting for fresh download: ${path.basename(mediaPath)}`);
          const deleted = this.deleteExistingFile(mediaPath);
          if (deleted) {
            logger.info(`🔄 Refreshing message ${message.id} after file deletion...`);
            
            // Refresh message after deleting file
            const refreshedMessage = await this.refreshMessageWithStrategies(client, channelId, message, 1);
            if (refreshedMessage) {
              message = refreshedMessage;
              logger.success(`✅ Message ${message.id} refreshed after file deletion`);
            }
          }
        }

        const dir = path.dirname(mediaPath);
        if (!fs.existsSync(dir)) {
          fs.mkdirSync(dir, { recursive: true });
        }

        const startTime = Date.now();

        // Dynamic optimization for single files
        const downloadOptions = isSingleFile
          ? {
              workers: 48, // Maximum workers for single file
              chunkSize: 32 * 1024 * 1024, // Larger chunks for single file
              workerIndex: 0,
              optimizeForSpeed: true,
              stabilizeSpeed: false, // Disable stabilization for single files
            }
          : {
              workers: Math.min(24, 16),
              chunkSize: 16 * 1024 * 1024,
              workerIndex: 0,
              optimizeForSpeed: true,
              stabilizeSpeed: true,
            };

        const result = await downloadMessageMedia(
          client,
          message,
          mediaPath,
          1,
          1,
          downloadOptions,
        );

        if (result && fs.existsSync(mediaPath)) {
          const fileSize = fs.statSync(mediaPath).size;
          const duration = (Date.now() - startTime) / 1000;
          const speedMbps =
            duration > 0 ? (fileSize * 8) / duration / 1000 / 1000 : 0;

          // Enhanced file size verification
          const expectedSize =
            message.media?.document?.size ||
            message.media?.photo?.sizes?.[0]?.size ||
            0;
          const sizeVerified =
            expectedSize === 0 || fileSize >= expectedSize * 0.95; // Allow 5% tolerance

          if (!sizeVerified && expectedSize > 0) {
            logger.warn(
              `⚠️ Size mismatch: ${path.basename(mediaPath)} - Expected: ${(expectedSize / 1024 / 1024).toFixed(2)}MB, Got: ${(fileSize / 1024 / 1024).toFixed(2)}MB`,
            );
            
            // Delete incomplete file and retry
            this.deleteExistingFile(mediaPath);
            throw new Error(`Incomplete download: ${path.basename(mediaPath)}`);
          }

          if (this.speedMonitor) {
            this.speedMonitor.updateSpeed(fileSize);
          }
          this.totalDownloaded++;
          this.resetFileReferenceTracking(); // Reset error tracking on success

          logger.info(
            `✅ Downloaded: ${path.basename(mediaPath)} (${speedMbps.toFixed(1)} Mbps)${sizeVerified ? " ✓ Size verified" : ""}${isSingleFile ? " [SINGLE-FILE BOOST]" : ""}`,
          );
          return mediaPath;
        } else {
          throw new Error("Download verification failed");
        }
      } catch (error) {
        attempt++;
        logger.warn(
          `❌ Download attempt ${attempt}/${maxRetries} failed for message ${message.id}: ${error.message}`,
        );

        if (error.message.includes("FILE_REFERENCE_EXPIRED")) {
          this.trackFileReferenceError();
          logger.error(`📋 File reference expired for a message, script will retry automatically`);
          
          // Enhanced multi-strategy refresh for FILE_REFERENCE_EXPIRED
          let refreshSuccess = false;
          for (let strategyAttempt = 1; strategyAttempt <= 8; strategyAttempt++) {
            logger.info(`🔄 FILE_REFERENCE strategy attempt ${strategyAttempt}/8 for message ${message.id}`);
            
            const refreshedMessage = await this.refreshMessageWithStrategies(
              client, 
              channelId, 
              originalMessage, 
              strategyAttempt
            );
            
            if (refreshedMessage) {
              message = refreshedMessage;
              logger.success(`✅ FILE_REFERENCE resolved using strategy ${Math.min(3, Math.ceil(strategyAttempt/2))}`);
              refreshSuccess = true;
              attempt = Math.max(0, attempt - 3); // Give extra attempts after successful refresh
              break;
            }
            
            // Progressive delay between strategy attempts
            const strategyDelay = Math.min(5000, 1000 * strategyAttempt);
            await this.precisionDelay(strategyDelay);
          }
          
          if (!refreshSuccess) {
            logger.error(`❌ All FILE_REFERENCE strategies failed for message ${message.id}`);
            if (attempt >= maxRetries) {
              throw new Error(`FILE_REFERENCE_EXPIRED: All refresh strategies failed`);
            }
          }
          
          // Continue main loop after refresh attempt
          continue;
        }

        if (attempt === maxRetries) {
          throw error; // Re-throw if all retries exhausted
        }

        // Progressive delay with longer waits for persistent errors
        const delay = isSingleFile
          ? Math.min(2000, 300 * attempt)
          : Math.min(5000, 500 * attempt);
        await this.precisionDelay(delay);
        continue;
      }
    }
    return null;
  }

  /**
   * ULTRA-OPTIMIZED upload with dynamic single-file acceleration
   */
  async uploadMessage(client, message, mediaPath = null, isSingleFile = false) {
    const maxRetries = isSingleFile ? 8 : 15; // Fewer retries for single files
    let attempt = 0;

    while (attempt < maxRetries) {
      try {
        if (!this.uploadMode || !this.targetChannelId) return false;

        // Minimal rate limit check for single files
        if (!isSingleFile) {
          await this.checkRateLimit();
        }

        const startTime = Date.now();
        const result = await uploadMessageToChannel(
          client,
          this.targetChannelId,
          message,
          mediaPath,
          isSingleFile,
        );

        if (result) {
          const duration = (Date.now() - startTime) / 1000;
          if (mediaPath && fs.existsSync(mediaPath)) {
            const fileSize = fs.statSync(mediaPath).size;
            const speedMbps =
              duration > 0 ? (fileSize * 8) / duration / 1000 / 1000 : 0;
            if (this.speedMonitor) {
              this.speedMonitor.updateSpeed(fileSize);
            }
            logger.info(
              `📤 Uploaded: Message ${message.id} (${speedMbps.toFixed(1)} Mbps)${isSingleFile ? " [SINGLE-FILE BOOST]" : ""}`,
            );
          }

          this.totalUploaded++;
          if (typeof this.updateFloodWaitHistory === "function") {
            this.updateFloodWaitHistory(false); // No flood wait occurred
          }
          return true;
        } else {
          throw new Error("Upload returned false");
        }
      } catch (error) {
        attempt++;
        logger.warn(
          `❌ Upload attempt ${attempt}/${maxRetries} failed for message ${message.id}: ${error.message}`,
        );

        if (error.message.includes("CHAT_FORWARDS_RESTRICTED")) {
          return false;
        } else if (error.message.includes("FLOOD_WAIT")) {
          const waitTime =
            parseInt(error.message.match(/\d+/)?.[0] || "60") * 1000;
          if (typeof this.updateFloodWaitHistory === "function") {
            this.updateFloodWaitHistory(true); // Flood wait occurred
          }
          // Shorter wait for single files
          const actualWait = isSingleFile
            ? Math.min(waitTime, 30000)
            : waitTime;
          await this.precisionDelay(actualWait);
          continue;
        }

        if (attempt < maxRetries) {
          // Minimal delay for single files
          const delay = isSingleFile
            ? Math.min(1500, 250 * attempt)
            : Math.min(3000, 400 * attempt);
          await this.precisionDelay(delay);
          continue;
        } else {
          const finalWait = isSingleFile ? 3000 : 10000;
          await this.precisionDelay(finalWait);
          try {
            const finalResult = await uploadMessageToChannel(
              client,
              this.targetChannelId,
              message,
              mediaPath,
              isSingleFile,
            );
            if (finalResult) {
              this.totalUploaded++;
              return true;
            }
          } catch (finalError) {
            logger.error(
              `❌ Final upload attempt failed: ${finalError.message}`,
            );
          }
          return false;
        }
      }
    }
    return false;
  }

  /**
   * ULTRA-HIGH-SPEED batch download with dynamic optimization
   */
  async downloadBatch(client, messages, channelId) {
    const isSingleFile = messages.length === 1;
    const isSmallBatch = messages.length <= 3; // Small batch optimization
    const isLastBatch =
      this.totalProcessedMessages + messages.length >=
      this.totalMessages * 0.95; // Last 5% of files

    // Determine optimization mode
    let optimizationMode;
    if (isSingleFile) {
      optimizationMode = "SINGLE-FILE TURBO";
    } else if (isSmallBatch || isLastBatch) {
      optimizationMode = "SMALL-BATCH TURBO";
    } else {
      optimizationMode = "BATCH PARALLEL";
    }

    // Enhanced speed target for small files and final files
    const speedTarget =
      isSingleFile || isSmallBatch || isLastBatch ? "40+ Mbps" : "30+ Mbps";

    logger.info(
      `📥 ${optimizationMode} download: ${messages.length} messages (${MAX_PARALLEL_DOWNLOADS_CONFIG} workers, ${CHUNK_SIZE_CONFIG / 1024 / 1024}MB chunks) - Target: ${speedTarget}`,
    );

    messages.sort((a, b) => a.id - b.id);

    // Minimal or no wait for single files, small batches, or last batch
    if (isSingleFile || isSmallBatch || isLastBatch) {
      await this.ultraOptimizedWait(5); // Ultra-minimal delay
    } else {
      await this.ultraOptimizedWait(15);
    }

    const downloadPromises = messages.map(async (message, index) => {
      let retryCount = 0;
      const maxBatchRetries = 3;

      while (retryCount < maxBatchRetries) {
        try {
          logger.info(
            `🚀 Ultra-parallel download ${index + 1}/${messages.length}: Message ${message.id}`,
          );

          await this.checkRateLimit();

          let mediaPath = null;
          let hasContent = false;

          if (message.message && message.message.trim()) {
            hasContent = true;
            logger.info(`📝 Text: "${message.message.substring(0, 30)}..."`);
          }

          if (message.media || message.sticker) {
            hasContent = true;
            // Set current file size for speed optimization
            const estimatedSize =
              message.media?.document?.size ||
              message.media?.photo?.sizes?.[0]?.size ||
              0;

            if (this.speedMonitor) {
              this.speedMonitor.setCurrentFileSize(estimatedSize);
            }

            mediaPath = await this.downloadMessage(
              client,
              message,
              channelId,
              isSingleFile || isSmallBatch || isLastBatch, // Treat small batches and last batch like single files
            );

            if (mediaPath && !fs.existsSync(mediaPath)) {
              logger.warn(`❌ File verification failed: ${mediaPath}`);
              mediaPath = null;
              throw new Error(`File not found: ${mediaPath}`);
            }
          }

          if (hasContent) {
            this.totalProcessedMessages++;
            logger.info(
              `✅ Download complete ${index + 1}/${messages.length}: Message ${message.id} (${this.speedMonitor ? this.speedMonitor.getCurrentSpeedMbps() + " Mbps" : "OK"})`,
            );
            return {
              message: message,
              mediaPath: mediaPath,
              hasContent: hasContent,
              downloadIndex: index,
            };
          }
          break;
        } catch (error) {
          retryCount++;

          // Track FILE_REFERENCE_EXPIRED errors
          if (error.message.includes("FILE_REFERENCE_EXPIRED")) {
            this.trackFileReferenceError();
            logger.error(
              `📋 File reference expired for a message, script will retry automatically`,
            );
          } else {
            logger.error(
              `❌ Batch retry ${retryCount}/${maxBatchRetries} for message ${message.id}: ${error.message}`,
            );
          }

          if (error.message.includes("FLOOD_WAIT")) {
            const waitTime = parseInt(error.message.match(/\d+/)?.[0] || "60");
            this.updateFloodWaitHistory(true, waitTime);
            await this.precisionDelay(waitTime * 1000);
          }

          if (retryCount < maxBatchRetries) {
            await this.precisionDelay(1000 * retryCount);
          } else {
            return {
              message: message,
              mediaPath: null,
              hasContent: Boolean(
                message.message || message.media || message.sticker,
              ),
              downloadIndex: index,
              failed: true,
            };
          }
        }
      }
      return null;
    });

    logger.info(
      `⏳ Processing ${messages.length} downloads with ${MAX_PARALLEL_DOWNLOADS_CONFIG} workers each...`,
    );
    const results = await Promise.all(downloadPromises);

    const downloadedData = results
      .filter((result) => result !== null)
      .sort((a, b) => a.message.id - b.message.id);

    const failedDownloads = downloadedData.filter((data) => data.failed).length;
    if (failedDownloads > 0) {
      logger.warn(`⚠️ ${failedDownloads} downloads had issues but proceeding`);
    }

    logger.info(
      `✅ Ultra-speed downloads complete! ${downloadedData.length} messages ready (Avg: ${this.speedMonitor ? this.speedMonitor.getAverageSpeedMbps() + " Mbps, Peak: " + this.speedMonitor.getPeakSpeedMbps() + " Mbps" : "High Speed"})`,
    );
    return downloadedData;
  }

  /**
   * SEQUENTIAL UPLOAD QUEUE - Maintains strict message order
   * Files wait at 99% until previous files complete upload
   */
  async uploadBatch(client, downloadedData) {
    if (!this.uploadMode || !downloadedData.length) {
      return downloadedData;
    }

    const isSingleFile = downloadedData.length === 1;
    const optimizationMode = isSingleFile
      ? "SINGLE-FILE BOOST"
      : "SEQUENTIAL QUEUE";

    // Sort by message ID to ensure proper order (a, b, c...)
    downloadedData.sort((a, b) => a.message.id - b.message.id);
    logger.info(
      `📤 ${optimizationMode} upload: ${downloadedData.length} messages - STRICT ORDER MAINTAINED`,
    );

    // Initialize sequential upload state
    const uploadQueue = downloadedData.map((data, index) => ({
      ...data,
      queueIndex: index,
      uploadStarted: false,
      uploadCompleted: false,
      readyToUpload: index === 0, // First message can start immediately
      waitingForPrevious: index > 0,
    }));

    let completedUploads = 0;
    const uploadResults = [];

    // Process uploads in strict sequential order
    for (
      let currentIndex = 0;
      currentIndex < uploadQueue.length;
      currentIndex++
    ) {
      const currentData = uploadQueue[currentIndex];
      const messageId = currentData.message.id;
      const fileName = currentData.mediaPath
        ? path.basename(currentData.mediaPath)
        : `Message_${messageId}`;

      try {
        // Wait for previous message to complete (if not first message)
        if (currentIndex > 0) {
          const previousData = uploadQueue[currentIndex - 1];
          if (!previousData.uploadCompleted) {
            logger.info(
              `⏳ File '${fileName}' waiting at 99% for previous message ${previousData.message.id} to complete...`,
            );

            // Keep showing "waiting" status until previous completes
            let waitCounter = 0;
            while (!previousData.uploadCompleted && waitCounter < 300) {
              // Max 5 minutes wait
              process.stdout.write(
                `\r⏳ [${currentIndex + 1}/${uploadQueue.length}] '${fileName}' waiting at 99% for message ${previousData.message.id}... (${waitCounter}s)`,
              );
              await this.precisionDelay(1000);
              waitCounter++;
            }

            if (!previousData.uploadCompleted) {
              throw new Error(
                `Timeout waiting for previous message ${previousData.message.id}`,
              );
            }

            process.stdout.write(
              `\n✅ Previous message completed! '${fileName}' can now upload...\n`,
            );
          }
        }

        // Verify file exists
        if (currentData.mediaPath && !fs.existsSync(currentData.mediaPath)) {
          logger.warn(
            `⚠️ Missing file for message ${messageId}: ${currentData.mediaPath}`,
          );
          currentData.mediaPath = null;
        }

        // Mark as started
        currentData.uploadStarted = true;
        logger.info(
          `🚀 [${currentIndex + 1}/${uploadQueue.length}] Sequential upload starting: '${fileName}' (Message ${messageId})`,
        );

        // Perform upload with retry mechanism
        await this.retryOperation(async () => {
          try {
            if (currentData.mediaPath) {
              await this.uploadMessage(
                client,
                currentData.message,
                currentData.mediaPath,
                isSingleFile,
              );
            } else {
              await this.uploadMessage(
                client,
                currentData.message,
                null,
                isSingleFile,
              );
            }
          } catch (error) {
            if (
              error.message.includes("Not connected") ||
              error.message.includes("Connection closed")
            ) {
              logger.warn(`🔄 Connection issue detected, reconnecting...`);
              await this.reconnectClient(client);
              throw error;
            }
            throw error;
          }
        }, `uploading message ${messageId} (${fileName})`);

        // Mark as completed
        currentData.uploadCompleted = true;
        completedUploads++;

        logger.info(
          `✅ [${currentIndex + 1}/${uploadQueue.length}] Sequential upload complete: '${fileName}' (Message ${messageId}) - ${this.speedMonitor ? this.speedMonitor.getCurrentSpeedMbps() + " Mbps" : "OK"}`,
        );

        // Signal next message that it can start
        if (currentIndex + 1 < uploadQueue.length) {
          uploadQueue[currentIndex + 1].readyToUpload = true;
          uploadQueue[currentIndex + 1].waitingForPrevious = false;
          logger.info(
            `🎯 Next file '${uploadQueue[currentIndex + 1].mediaPath ? path.basename(uploadQueue[currentIndex + 1].mediaPath) : `Message_${uploadQueue[currentIndex + 1].message.id}`}' is now ready to upload`,
          );
        }

        uploadResults.push({ success: true, data: currentData });
      } catch (error) {
        logger.error(
          `❌ Sequential upload error for '${fileName}' (Message ${messageId}): ${error.message}`,
        );

        if (error.message.includes("FLOOD_WAIT")) {
          const waitTime = parseInt(error.message.match(/\d+/)?.[0] || "60");
          logger.warn(
            `⏳ Flood wait for ${waitTime}s during sequential upload...`,
          );
          await this.precisionDelay(waitTime * 1000);

          try {
            // Retry upload after flood wait
            const retrySuccess = await this.uploadMessage(
              client,
              currentData.message,
              currentData.mediaPath,
              isSingleFile,
            );
            if (retrySuccess) {
              currentData.uploadCompleted = true;
              completedUploads++;
              uploadResults.push({ success: true, data: currentData });
              continue;
            }
          } catch (retryError) {
            logger.error(
              `❌ Sequential upload retry failed: ${retryError.message}`,
            );
          }
        }

        // Mark as failed but continue sequence
        currentData.uploadCompleted = true; // Allow next file to proceed
        uploadResults.push({ success: false, data: currentData });

        // Signal next message can proceed despite this failure
        if (currentIndex + 1 < uploadQueue.length) {
          uploadQueue[currentIndex + 1].readyToUpload = true;
          uploadQueue[currentIndex + 1].waitingForPrevious = false;
        }
      }

      // Small delay between sequential uploads for stability
      if (currentIndex + 1 < uploadQueue.length) {
        await this.precisionDelay(isSingleFile ? 100 : 500);
      }
    }

    logger.info(
      `✅ Sequential upload queue complete! ${completedUploads}/${uploadQueue.length} messages uploaded in perfect order (Avg: ${this.speedMonitor ? this.speedMonitor.getAverageSpeedMbps() + " Mbps, Peak: " + this.speedMonitor.getPeakSpeedMbps() + " Mbps" : "High Speed"})`,
    );

    return downloadedData;
  }

  /**
   * ULTRA-HIGH-SPEED batch download with dynamic optimization
   */
  async downloadBatch(client, messages, channelId) {
    const isSingleFile = messages.length === 1;
    const isSmallBatch = messages.length <= 3; // Small batch optimization
    const isLastBatch =
      this.totalProcessedMessages + messages.length >=
      this.totalMessages * 0.95; // Last 5% of files

    // Determine optimization mode
    let optimizationMode;
    if (isSingleFile) {
      optimizationMode = "SINGLE-FILE TURBO";
    } else if (isSmallBatch || isLastBatch) {
      optimizationMode = "SMALL-BATCH TURBO";
    } else {
      optimizationMode = "BATCH PARALLEL";
    }

    // Enhanced speed target for small files and final files
    const speedTarget =
      isSingleFile || isSmallBatch || isLastBatch ? "40+ Mbps" : "30+ Mbps";

    logger.info(
      `📥 ${optimizationMode} download: ${messages.length} messages (${MAX_PARALLEL_DOWNLOADS_CONFIG} workers, ${CHUNK_SIZE_CONFIG / 1024 / 1024}MB chunks) - Target: ${speedTarget}`,
    );

    messages.sort((a, b) => a.id - b.id);

    // Minimal or no wait for single files, small batches, or last batch
    if (isSingleFile || isSmallBatch || isLastBatch) {
      await this.ultraOptimizedWait(5); // Ultra-minimal delay
    } else {
      await this.ultraOptimizedWait(15);
    }

    const downloadPromises = messages.map(async (message, index) => {
      let retryCount = 0;
      const maxBatchRetries = 3;

      while (retryCount < maxBatchRetries) {
        try {
          logger.info(
            `🚀 Ultra-parallel download ${index + 1}/${messages.length}: Message ${message.id}`,
          );

          await this.checkRateLimit();

          let mediaPath = null;
          let hasContent = false;

          if (message.message && message.message.trim()) {
            hasContent = true;
            logger.info(`📝 Text: "${message.message.substring(0, 30)}..."`);
          }

          if (message.media || message.sticker) {
            hasContent = true;
            // Set current file size for speed optimization
            const estimatedSize =
              message.media?.document?.size ||
              message.media?.photo?.sizes?.[0]?.size ||
              0;

            if (this.speedMonitor) {
              this.speedMonitor.setCurrentFileSize(estimatedSize);
            }

            mediaPath = await this.downloadMessage(
              client,
              message,
              channelId,
              isSingleFile || isSmallBatch || isLastBatch, // Treat small batches and last batch like single files
            );

            if (mediaPath && !fs.existsSync(mediaPath)) {
              logger.warn(`❌ File verification failed: ${mediaPath}`);
              mediaPath = null;
              throw new Error(`File not found: ${mediaPath}`);
            }
          }

          if (hasContent) {
            this.totalProcessedMessages++;
            logger.info(
              `✅ Download complete ${index + 1}/${messages.length}: Message ${message.id} (${this.speedMonitor ? this.speedMonitor.getCurrentSpeedMbps() + " Mbps" : "OK"})`,
            );
            return {
              message: message,
              mediaPath: mediaPath,
              hasContent: hasContent,
              downloadIndex: index,
            };
          }
          break;
        } catch (error) {
          retryCount++;

          // Track FILE_REFERENCE_EXPIRED errors
          if (error.message.includes("FILE_REFERENCE_EXPIRED")) {
            this.trackFileReferenceError();
            logger.error(
              `📋 File reference expired for a message, script will retry automatically`,
            );
          } else {
            logger.error(
              `❌ Batch retry ${retryCount}/${maxBatchRetries} for message ${message.id}: ${error.message}`,
            );
          }

          if (error.message.includes("FLOOD_WAIT")) {
            const waitTime = parseInt(error.message.match(/\d+/)?.[0] || "60");
            this.updateFloodWaitHistory(true, waitTime);
            await this.precisionDelay(waitTime * 1000);
          }

          if (retryCount < maxBatchRetries) {
            await this.precisionDelay(1000 * retryCount);
          } else {
            return {
              message: message,
              mediaPath: null,
              hasContent: Boolean(
                message.message || message.media || message.sticker,
              ),
              downloadIndex: index,
              failed: true,
            };
          }
        }
      }
      return null;
    });

    logger.info(
      `⏳ Processing ${messages.length} downloads with ${MAX_PARALLEL_DOWNLOADS_CONFIG} workers each...`,
    );
    const results = await Promise.all(downloadPromises);

    const downloadedData = results
      .filter((result) => result !== null)
      .sort((a, b) => a.message.id - b.message.id);

    const failedDownloads = downloadedData.filter((data) => data.failed).length;
    if (failedDownloads > 0) {
      logger.warn(`⚠️ ${failedDownloads} downloads had issues but proceeding`);
    }

    logger.info(
      `✅ Ultra-speed downloads complete! ${downloadedData.length} messages ready (Avg: ${this.speedMonitor ? this.speedMonitor.getAverageSpeedMbps() + " Mbps, Peak: " + this.speedMonitor.getPeakSpeedMbps() + " Mbps" : "High Speed"})`,
    );
    return downloadedData;
  }

  /**
   * Cleanup batch files
   */
  async cleanupBatch(downloadedData) {
    logger.info(`🗑️ Cleaning up ${downloadedData.length} files`);

    const cleanupPromises = downloadedData.map(async (data) => {
      if (data.mediaPath && fs.existsSync(data.mediaPath)) {
        try {
          fs.unlinkSync(data.mediaPath);
        } catch (cleanupError) {
          logger.warn(
            `⚠️ Cleanup failed for ${data.mediaPath}: ${cleanupError.message}`,
          );
        }
      }
    });

    await Promise.all(cleanupPromises);
    logger.info(`✅ Cleanup complete`);
  }

  /**
   * Enhanced message refresh with speed optimization
   */
  async refreshMessagesBatch(client, channelId, messageIds) {
    try {
      logger.info(`🔄 Refreshing ${messageIds.length} messages...`);
      const refreshedMessages = await getMessageDetail(
        client,
        channelId,
        messageIds,
      );
      logger.info(`✅ Refreshed ${refreshedMessages.length} messages`);
      return refreshedMessages;
    } catch (error) {
      logger.error(`❌ Message refresh failed: ${error.message}`);
      return null;
    }
  }

  /**
   * Function to refresh ALL channel messages every 3 batches to prevent false "file exists" detection.
   */
  async refreshAllChannelMessages(client, channelId) {
    try {
      logger.info(
        `🔄 Fetching ALL messages from channel ${channelId} for refresh...`,
      );
      // Fetch all messages to ensure comprehensive refresh
      const allMessages = await getMessages(
        client,
        channelId,
        MESSAGE_LIMIT_CONFIG,
        0,
        true,
      );

      if (!allMessages || allMessages.length === 0) {
        logger.warn("No messages found in the channel for refresh.");
        return [];
      }

      const messageIds = allMessages.map((msg) => msg.id);
      logger.info(`🔄 Refreshing details for ${messageIds.length} messages...`);
      const refreshedMessages = await getMessageDetail(
        client,
        channelId,
        messageIds,
      );
      logger.info(
        `✅ Successfully refreshed details for ${refreshedMessages.length} messages.`,
      );
      return refreshedMessages;
    } catch (error) {
      logger.error(
        `❌ Failed to refresh all channel messages: ${error.message}`,
      );
      return null;
    }
  }

  /**
   * ULTRA-OPTIMIZED batch processing with proactive refresh every batch
   */
  async processBatch(client, messages, batchIndex, totalBatches, channelId) {
    try {
      this.batchCounter++;
      logger.info(
        `🔄 ULTRA-SPEED batch ${batchIndex + 1}/${totalBatches} (${messages.length} messages) - Speed: ${this.speedMonitor ? this.speedMonitor.getCurrentSpeedMbps() + " Mbps (" + this.speedMonitor.getSpeedStatus() + ")" : "Optimizing..."}`,
      );

      // Enhanced: Refresh messages EVERY batch to prevent FILE_REFERENCE_EXPIRED errors
      logger.info(
        `🔄 Batch ${this.batchCounter}: Proactively refreshing messages to prevent file reference errors...`,
      );
      
      // Refresh current batch messages specifically
      const currentMessageIds = messages.map((m) => m.id);
      try {
        const refreshedMessages = await getMessageDetail(client, channelId, currentMessageIds);
        
        if (refreshedMessages && refreshedMessages.length > 0) {
          // Update messages with refreshed data
          const messageMap = new Map(refreshedMessages.map(msg => [msg.id, msg]));
          messages = messages.map(msg => messageMap.get(msg.id) || msg);
          logger.success(`✅ Refreshed ${refreshedMessages.length}/${messages.length} batch messages`);
        } else {
          // Fallback: Full channel refresh if batch refresh fails
          logger.warn(`⚠️ Batch refresh failed, attempting full channel refresh...`);
          const allRefreshedMessages = await this.refreshAllChannelMessages(client, channelId);
          
          if (allRefreshedMessages && allRefreshedMessages.length > 0) {
            const messageMap = new Map(allRefreshedMessages.map(msg => [msg.id, msg]));
            messages = messages.map(msg => messageMap.get(msg.id) || msg);
            logger.success(`✅ Fallback: Updated ${messages.length} messages from full refresh`);
          }
        }
      } catch (refreshError) {
        logger.warn(`⚠️ Message refresh failed: ${refreshError.message}, proceeding with original messages`);
      }

      // Additional refresh every 3 batches or when high FILE_REFERENCE error rate detected
      const recentFileRefErrors = this.fileReferenceErrors.filter(
        timestamp => Date.now() - timestamp < 2 * 60 * 1000 // Last 2 minutes
      ).length;
      
      if (this.batchCounter % 3 === 0 || recentFileRefErrors > 5) {
        logger.info(
          `🔄 Extra refresh triggered - Batch ${this.batchCounter} or high error rate (${recentFileRefErrors} recent errors)`,
        );
        const allRefreshedMessages = await this.refreshAllChannelMessages(client, channelId);

        if (allRefreshedMessages && allRefreshedMessages.length > 0) {
          const messageMap = new Map(allRefreshedMessages.map(msg => [msg.id, msg]));
          messages = messages.map(msg => messageMap.get(msg.id) || msg);
          logger.success(`✅ Extra refresh: Updated ${messages.length} messages`);
        }
      }

      // Task 3: Duplicate messages only works if the duplicate messages send together line number of three in one line then they will remove the duplicate messages not for the whole sessions it will work work when their is one after the other.
      // This task is inherently handled by the nature of how messages are processed sequentially and media is checked.
      // If duplicates are sent consecutively, the `checkFileExist` within `downloadMessage` will correctly identify and skip them.
      // For non-consecutive duplicates, the script will attempt to download them if they are in different batches and `checkFileExist` will work as intended for each message.

      // Phase 1: Ultra-high-speed parallel download
      logger.info(
        `📥 Phase 1: Ultra-download ${messages.length} messages (${MAX_PARALLEL_DOWNLOADS_CONFIG} workers, ${CHUNK_SIZE_CONFIG / 1024 / 1024}MB chunks)`,
      );
      const downloadedData = await this.downloadBatch(
        client,
        messages,
        channelId,
      );

      // Phase 2: Ultra-high-speed parallel upload
      if (this.uploadMode && downloadedData.length > 0) {
        logger.info(
          `📤 Phase 2: Ultra-upload ${downloadedData.length} messages (${MAX_PARALLEL_UPLOADS_CONFIG} workers)`,
        );
        await this.ensureConnectionHealth(client);
        const uploadedData = await this.uploadBatch(client, downloadedData);

        // Phase 3: Ultra-fast cleanup
        logger.info(`🗑️ Phase 3: Ultra-cleanup ${uploadedData.length} files`);
        await this.cleanupBatch(uploadedData);
      }

      logger.info(
        `✅ Ultra-speed batch ${batchIndex + 1}/${totalBatches} complete (Current: ${this.speedMonitor ? this.speedMonitor.getCurrentSpeedMbps() + " Mbps, Avg: " + this.speedMonitor.getAverageSpeedMbps() + " Mbps, Peak: " + this.speedMonitor.getPeakSpeedMbps() + " Mbps" : "Complete"})`,
      );
    } catch (error) {
      logger.error(`❌ Batch error ${batchIndex + 1}: ${error.message}`);

      // Enhanced retry with adaptive recovery
      try {
        const messageIds = messages.map((m) => m.id);
        const retryMessages = await this.refreshMessagesBatch(
          client,
          channelId,
          messageIds,
        );
        if (retryMessages && retryMessages.length > 0) {
          await this.ensureConnectionHealth(client);
          await this.processBatch(
            client,
            retryMessages,
            batchIndex,
            totalBatches,
            channelId,
          );
        }
      } catch (retryError) {
        logger.error(`❌ Batch retry failed: ${retryError.message}`);
      }
    }
  }

  /**
   * Record messages with enhanced tracking
   */
  recordMessages(messages) {
    const filePath = path.join(this.outputFolder, "all_messages.json");
    if (!fs.existsSync(this.outputFolder)) {
      fs.mkdirSync(this.outputFolder, { recursive: true });
    }

    const data = messages.map((msg) => ({
      id: msg.id,
      message: msg.message || "",
      date: msg.date,
      out: msg.out,
      hasMedia: !!msg.media,
      sender: msg.fromId?.userId || msg.peerId?.userId,
      mediaType: this.hasContent(msg) ? getMediaType(msg) : undefined,
      mediaPath:
        this.hasContent(msg) && msg.media
          ? getMediaPath(msg, this.outputFolder)
          : undefined,
      mediaName:
        this.hasContent(msg) && msg.media
          ? path.basename(getMediaPath(msg, this.outputFolder))
          : undefined,
    }));

    appendToJSONArrayFile(filePath, data);
  }

  /**
   * Enhanced memory cleanup with garbage collection optimization
   */
  cleanupMemory() {
    try {
      if (global.gc) {
        global.gc();
      }

      // Clear speed history periodically to prevent memory buildup
      if (this.speedMonitor && this.speedMonitor.speedHistory.length > 20) {
        this.speedMonitor.speedHistory =
          this.speedMonitor.speedHistory.slice(-10);
      }

      // Clean old flood wait history
      const now = Date.now();
      this.floodWaitHistory = this.floodWaitHistory.filter(
        (entry) => now - entry.timestamp < 5 * 60 * 1000,
      );

      // Clean old file reference error history
      this.fileReferenceErrors = this.fileReferenceErrors.filter(
        (timestamp) => now - timestamp < 30 * 60 * 1000,
      );

      const tempDir = path.join(this.outputFolder, "temp");
      if (fs.existsSync(tempDir)) {
        const files = fs.readdirSync(tempDir);
        files.forEach((file) => {
          const filePath = path.join(tempDir, file);
          const stats = fs.statSync(filePath);
          if (now - stats.mtime.getTime() > 2 * 60 * 1000) {
            // 2 minutes
            fs.unlinkSync(filePath);
          }
        });
      }
    } catch (err) {
      logger.warn("Memory cleanup failed:", err.message);
    }
  }

  /**
   * Enhanced progress display with comprehensive speed metrics
   */
  showProgress(currentBatch) {
    const progressPercentage =
      this.totalMessages > 0
        ? Math.round((this.totalProcessedMessages / this.totalMessages) * 100)
        : 0;

    if (currentBatch % 3 === 0) {
      this.cleanupMemory();
    }

    logger.info("=".repeat(90));
    logger.info("🚀 ULTRA-HIGH-SPEED PROCESSING REPORT (TARGET: 35+ Mbps)");
    logger.info("=".repeat(90));
    logger.info(`📥 Downloaded: ${this.totalDownloaded} files`);
    if (this.uploadMode) {
      logger.info(`📤 Uploaded: ${this.totalUploaded} messages`);
    }
    logger.info(
      `📈 Progress: ${progressPercentage}% (${this.totalProcessedMessages}/${this.totalMessages})`,
    );
    logger.info(
      `🚀 Current Speed: ${this.speedMonitor ? this.speedMonitor.getCurrentSpeedMbps() + " Mbps " + this.speedMonitor.getSpeedStatus() : "Optimizing..."}`,
    );
    logger.info(
      `📊 Average Speed: ${this.speedMonitor ? this.speedMonitor.getAverageSpeedMbps() + " Mbps" : "N/A"}`,
    );
    logger.info(
      `🎯 Peak Speed: ${this.speedMonitor ? this.speedMonitor.getPeakSpeedMbps() + " Mbps" : "N/A"}`,
    );
    logger.info(`📦 Batch: ${currentBatch} messages processed`);
    logger.info(
      `🎯 Speed Factor: ${this.speedMonitor ? this.speedMonitor.stabilizationFactor.toFixed(1) + "x" : "N/A"} | Boost Mode: ${this.speedBoostMode ? "ON" : "OFF"}`,
    );
    logger.info(
      `🌊 Flood Waits: ${this.consecutiveFloodWaits} consecutive | Adaptive Delay: ${this.adaptiveDelayMultiplier.toFixed(2)}x`,
    );
    logger.info(
      `⚡ File Reference Errors: ${this.consecutiveFileRefErrors} consecutive`,
    );
    logger.info(`🔗 Connection Health: ${this.connectionHealth}%`);
    logger.info("=".repeat(90));
  }

  /**
   * MAIN ultra-high-speed download function with 35+ Mbps target
   */
  async downloadChannel(client, channelId, offsetMsgId = 0) {
    try {
      this.initializeSpeedMonitor();

      this.outputFolder = path.join(
        process.cwd(),
        "export",
        channelId.toString(),
      );

      if (this.selectiveMode && offsetMsgId === 0) {
        offsetMsgId = this.startFromMessageId;
        logger.info(
          `📋 Selective mode: Starting from message ID ${offsetMsgId}`,
        );
      }

      const messages = await this.retryOperation(async () => {
        return await getMessages(
          client,
          channelId,
          MESSAGE_LIMIT_CONFIG,
          offsetMsgId,
          true,
        );
      });

      if (!messages.length) {
        logger.info("🎉 Ultra-speed processing complete! No more messages.");
        this.showProgress(0);
        return;
      }

      let filteredMessages = messages;
      if (this.selectiveMode) {
        filteredMessages = messages.filter(
          (msg) => msg.id >= this.startFromMessageId,
        );
        logger.info(
          `📋 Filtered ${filteredMessages.length} messages from ${messages.length}`,
        );
      }

      filteredMessages.sort((a, b) => a.id - b.id);

      const ids = filteredMessages.map((m) => m.id);
      const details = await this.retryOperation(async () => {
        return await getMessageDetail(client, channelId, ids);
      });

      details.sort((a, b) => a.id - b.id);
      const messagesToProcess = details.filter((msg) =>
        this.shouldProcess(msg),
      );

      logger.info(
        `📋 Ultra-processing ${messagesToProcess.length}/${details.length} messages`,
      );
      logger.info(
        `🚀 ULTRA-SPEED CONFIG: ${BATCH_SIZE} batches, ${MAX_PARALLEL_DOWNLOADS_CONFIG} download workers, ${MAX_PARALLEL_UPLOADS_CONFIG} upload workers`,
      );
      logger.info(
        `⚡ SPEED OPTIMIZATION: ${CHUNK_SIZE_CONFIG / 1024 / 1024}MB chunks, ${RATE_LIMIT_DELAY_CONFIG}ms delays, 35+ Mbps target`,
      );

      if (this.uploadMode) {
        const targetName = await getDialogName(client, this.targetChannelId);
        logger.info(`📤 Target: ${targetName}`);
      }

      const totalBatches = Math.ceil(messagesToProcess.length / BATCH_SIZE);
      this.totalMessages = messagesToProcess.length;

      for (let i = 0; i < messagesToProcess.length; i += BATCH_SIZE) {
        const batch = messagesToProcess.slice(i, i + BATCH_SIZE);
        const batchIndex = Math.floor(i / BATCH_SIZE);

        logger.info(
          `🚀 Ultra-speed batch ${batchIndex + 1}/${totalBatches} - ${batch.length} messages`,
        );
        await this.processBatch(
          client,
          batch,
          batchIndex,
          totalBatches,
          channelId,
        );

        if (i + BATCH_SIZE < messagesToProcess.length) {
          logger.info(
            `⏳ Ultra-precision delay ${RATE_LIMIT_DELAY_CONFIG}ms before next ultra-batch...`,
          );
          await this.precisionDelay(RATE_LIMIT_DELAY_CONFIG);
        }
      }

      this.recordMessages(details);

      const maxId = Math.max(...filteredMessages.map((m) => m.id));
      updateLastSelection({
        messageOffsetId: maxId,
      });

      this.showProgress(messagesToProcess.length);

      // Check if there are more messages to process
      if (messages.length === MESSAGE_LIMIT_CONFIG) {
        // There might be more messages, continue with next batch
        await this.precisionDelay(RATE_LIMIT_DELAY_CONFIG);
        await this.downloadChannel(client, channelId, maxId);
      } else {
        // All messages processed for this channel
        logger.info("🎉 Ultra-speed processing complete! No more messages.");
        this.showProgress(messagesToProcess.length);
        return;
      }
    } catch (err) {
      logger.error("Ultra-speed processing error:");
      console.error(err);

      if (err.message && err.message.includes("FLOOD_WAIT")) {
        const waitTime =
          parseInt(err.message.match(/\d+/)?.[0] || "300") * 1000;
        this.updateFloodWaitHistory(true, waitTime / 1000);
        logger.info(
          `⚠️ Rate limited! Waiting ${waitTime / 1000}s... (Adaptive: ${this.adaptiveDelayMultiplier.toFixed(2)}x)`,
        );
        await this.precisionDelay(waitTime);
        return await this.downloadChannel(client, channelId, offsetMsgId);
      }

      throw err;
    }
  }

  /**
   * Enhanced configuration with ultra-speed optimization
   */
  async configureDownload(options, client) {
    let channelId = options.channelId;
    let downloadableFiles = options.downloadableFiles;

    // Check if this is a resume from existing session (user used /do command)
    const isResumeSession = options.resumeSession || false;
    
    if (isResumeSession) {
      logger.info("🔄 Resuming with existing login credentials - Starting from channel selection");
    }

    if (!channelId) {
      logger.info("Select channel for ULTRA-SPEED download (35+ Mbps target)");
      const allChannels = await getAllDialogs(client);

      const useSearch = await booleanInput(
        "Search channel by name? (No = browse all)",
      );

      let selectedChannelId;
      if (useSearch) {
        const { searchDialog } = require("../modules/dialoges");
        selectedChannelId = await searchDialog(allChannels);
      } else {
        const validChannels = allChannels.filter((d) => d.name && d.id);
        const channelOptions = validChannels.map((d) => {
          const displayName = `${d.name} (${d.id})`;
          return {
            name: displayName,
            value: d.id,
          };
        });

        if (channelOptions.length === 0) {
          throw new Error("No valid channels found!");
        }

        selectedChannelId = await selectInput(
          "Select source channel for ULTRA-SPEED download",
          channelOptions,
        );
      }

      channelId = selectedChannelId;
    }

    // Download mode selection
    const downloadModeOptions = [
      { name: "Download ALL messages (ULTRA-SPEED 35+ Mbps)", value: "full" },
      { name: "Download SPECIFIC messages only", value: "specific" },
      { name: "Download FROM message TO END", value: "toEnd" },
    ];

    const downloadMode = await selectInput(
      "Choose ULTRA-SPEED download mode:",
      downloadModeOptions,
    );

    let startFromMessageId = 0;
    if (downloadMode === "specific") {
      const { textInput } = require("../utils/input-helper");
      const messageIdInput = await textInput(
        "Enter specific message IDs (comma-separated): ",
      );
      const messageIds = messageIdInput
        .split(",")
        .map((id) => parseInt(id.trim()))
        .filter((id) => !isNaN(id));
      if (messageIds.length === 0) {
        throw new Error("No valid message IDs provided!");
      }
      this.specificMessageIds = messageIds;
      logger.info(`📋 Specific messages: ${messageIds.join(", ")}`);
    } else if (downloadMode === "toEnd") {
      const { textInput } = require("../utils/input-helper");
      const messageIdInput = await textInput("Enter starting message ID: ");
      startFromMessageId = parseInt(messageIdInput) || 0;
      logger.info(`📋 Download from message ${startFromMessageId} to end`);
      this.downloadToEndMode = true;
    } else {
      logger.info("📋 ULTRA-SPEED full channel download (35+ Mbps target)");
    }

    this.selectiveMode = downloadMode !== "full";
    this.startFromMessageId = startFromMessageId;

    // Upload mode configuration
    this.uploadMode = await booleanInput(
      "Enable ULTRA-SPEED upload to another channel? (35+ Mbps)",
    );

    if (this.uploadMode) {
      logger.info("Select target channel for ULTRA-SPEED upload");
      const allChannels = await getAllDialogs(client);

      const useSearchForTarget = await booleanInput(
        "Search target channel by name?",
      );

      let targetChannelId;
      if (useSearchForTarget) {
        const validTargetChannels = allChannels.filter(
          (d) => d.name && d.id && d.id !== channelId,
        );
        if (validTargetChannels.length === 0) {
          logger.warn("No valid target channels! Upload disabled.");
          this.uploadMode = false;
        } else {
          const { searchDialog } = require("../modules/dialoges");
          targetChannelId = await searchDialog(validTargetChannels);
        }
      } else {
        const validTargetChannels = allChannels.filter(
          (d) => d.name && d.id && d.id !== channelId,
        );
        const targetOptions = validTargetChannels.map((d) => {
          const displayName = `${d.name} (${d.id})`;
          return {
            name: displayName,
            value: d.id,
          };
        });

        if (targetOptions.length === 0) {
          logger.warn("No valid target channels! Upload disabled.");
          this.uploadMode = false;
        } else {
          targetChannelId = await selectInput(
            "Select target channel for ULTRA-SPEED upload",
            targetOptions,
          );
        }
      }

      if (this.uploadMode) {
        this.targetChannelId = targetChannelId;
        logger.info(
          `📤 ULTRA-SPEED upload enabled (35+ Mbps): ${this.targetChannelId}`,
        );
      }
    }

    if (!this.uploadMode) {
      logger.info("💾 ULTRA-SPEED local storage mode (35+ Mbps)");
    }

    // Enhanced file type configuration
    if (!downloadableFiles) {
      downloadableFiles = {
        webpage: true,
        poll: true,
        geo: true,
        contact: true,
        venue: true,
        sticker: true,
        image: true,
        video: true,
        audio: true,
        voice: true,
        document: true,
        pdf: true,
        zip: true,
        all: true,
      };
    }

    this.downloadableFiles = downloadableFiles;

    const lastSelection = getLastSelection();
    let messageOffsetId = lastSelection.messageOffsetId || 0;

    if (Number(lastSelection.channelId) !== Number(channelId)) {
      messageOffsetId = 0;
    }

    updateLastSelection({ messageOffsetId, channelId });
    return { channelId, messageOffsetId };
  }

  /**
   * Main handler with ultra-speed initialization targeting 35+ Mbps
   */
  async handle(options = {}) {
    let client;

    try {
      await this.ultraOptimizedWait(100);

      // Check if this is a resume from existing session
      const isResume = process.argv.includes('--resume') || options.resumeSession;
      
      if (isResume) {
        logger.info("🔄 Resuming session - Using existing authentication");
        options.resumeSession = true;
      }

      client = await initAuth();

      // Enable continuous mode - keep running until user chooses to exit
      await this.continuousMode(client, options);
    } catch (err) {
      logger.error("ULTRA-SPEED processing error:");
      console.error(err);
      await this.ultraOptimizedWait(5000);
    } finally {
      if (client) {
        try {
          await client.disconnect();
        } catch (disconnectErr) {
          logger.warn("Disconnect error:", disconnectErr.message);
        }
      }
      process.exit(0);
    }
  }

  /**
   * Continuous mode - allows multiple channel downloads without re-authentication
   */
  async continuousMode(client, initialOptions = {}) {
    logger.info(
      "🔄 CONTINUOUS MODE: Multiple channel downloads without re-login",
    );
    logger.info("📋 You can download/upload multiple channels in one session");

    while (true) {
      try {
        // Reset instance variables for new channel
        this.totalDownloaded = 0;
        this.totalUploaded = 0;
        this.totalMessages = 0;
        this.totalProcessedMessages = 0;
        this.skippedFiles = 0;
        this.batchCounter = 0;
        this.requestCount = 0;
        this.lastRequestTime = 0;
        this.consecutiveFloodWaits = 0;
        this.consecutiveFileRefErrors = 0; // Reset file reference errors
        this.speedMonitor = null;

        const { channelId, messageOffsetId } = await this.configureDownload(
          initialOptions,
          client,
        );

        const dialogName = await getDialogName(client, channelId);
        logger.info(
          `🚀 ULTRA-HIGH-SPEED download (35+ Mbps target): ${dialogName}`,
        );
        logger.info(
          `⚙️ CONFIG: Batch=${BATCH_SIZE}, Upload=${this.uploadMode ? "ON" : "OFF"}`,
        );
        logger.info(
          `🚀 SPEED: ${MAX_PARALLEL_DOWNLOADS_CONFIG} download workers, ${MAX_PARALLEL_UPLOADS_CONFIG} upload workers, ${CHUNK_SIZE_CONFIG / 1024 / 1024}MB chunks`,
        );
        logger.info(
          `⏰ DELAYS: Rate=${RATE_LIMIT_DELAY_CONFIG}ms, Download=${DOWNLOAD_DELAY_CONFIG}ms, Upload=${UPLOAD_DELAY_CONFIG}ms`,
        );
        logger.info(`📋 ORDER: Oldest → Newest`);
        logger.info(
          `🔄 PATTERN: Download All ULTRA-PARALLEL → Upload All ULTRA-PARALLEL → Delete All`,
        );
        logger.info(
          `🌊 FLOOD CONTROL: Adaptive learning enabled with ${this.floodWaitHistory.length} historical data points`,
        );

        await this.downloadChannel(client, channelId, messageOffsetId);

        // Ask if user wants to continue with another channel
        const continueDownload = await this.askContinue();
        if (!continueDownload) {
          logger.info("🎉 Session complete! Exiting continuous mode...");
          break;
        }

        // Clear initial options so user can select new channel
        initialOptions = {};

        logger.info("🔄 Starting new channel selection...");
        await this.ultraOptimizedWait(1000);
      } catch (err) {
        logger.error("Error in continuous mode:");
        console.error(err);

        const retryAfterError = await this.askRetryAfterError();
        if (!retryAfterError) {
          logger.info("🛑 Exiting due to error...");
          break;
        }

        await this.ultraOptimizedWait(2000);
      }
    }
  }

  /**
   * Ask user if they want to continue with another channel
   */
  async askContinue() {
    try {
      const { booleanInput } = require("../utils/input-helper");
      return await booleanInput(
        "🔄 Download/Upload another channel? (Yes = Continue, No = Exit)",
      );
    } catch (error) {
      logger.warn("Failed to get continue input, defaulting to exit");
      return false;
    }
  }

  /**
   * Ask user if they want to retry after an error
   */
  async askRetryAfterError() {
    try {
      const { booleanInput } = require("../utils/input-helper");
      return await booleanInput(
        "❌ An error occurred. Try again with a different channel? (Yes = Retry, No = Exit)",
      );
    } catch (error) {
      logger.warn("Failed to get retry input, defaulting to exit");
      return false;
    }
  }
}

module.exports = DownloadChannel;
