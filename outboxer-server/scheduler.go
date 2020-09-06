package outboxer_server

// user ticker to schedule the polling on the poller struct
// use select with a default clause so that if the poller cannot receive the message as it is already running then the default clause will hit
// we only want one instance of the poller processing items at a time
// https://gobyexample.com/non-blocking-channel-operations

// also provide ability for client to call to schedule immediately and call for an immediate poll


