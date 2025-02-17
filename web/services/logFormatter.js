/**
 * Build enriched log data.
 * @param {object} userInfo - The Okta userinfo payload
 * @returns {object} - The enriched log data
 */

const { getFormattedDateTime } = require('./dateTimeHelper')

function buildLogData(userInfo) {
    const timestamp = getFormattedDateTime();
    const podName = process.env.POD_NAME || "unknown-pod";
    
    return {
      timestamp,
      podName,
      username: userInfo.name,
      locale: userInfo.locale,
      email: userInfo.email,
      zoneinfo: userInfo.zoneinfo,
      email_verified: userInfo.email_verified
    };
  }
  
  module.exports = { buildLogData };