function getFormattedDateTime() {
    const d = new Date();
    const pad = (n, size = 2) => n.toString().padStart(size, '0');
    const year = d.getFullYear();
    const month = pad(d.getMonth() + 1);
    const day = pad(d.getDate());
    const hour = pad(d.getHours());
    const minute = pad(d.getMinutes());
    const second = pad(d.getSeconds());
    const ms = pad(d.getMilliseconds(), 3);
    return `${year}-${month}-${day} ${hour}:${minute}:${second}.${ms}`;
  }
  
  module.exports = { getFormattedDateTime };