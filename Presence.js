const mongoose = require('mongoose');

const presenceSchema = new mongoose.Schema({
  userId: { type: String, required: true },
  status: String,
  client_status: Object,
  activities: Array,
  updatedAt: { type: Date, default: Date.now }
});

module.exports = mongoose.model('Presence', presenceSchema);
