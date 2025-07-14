




pub(crate) fn sim_network_stack<M: AsRef<[Machine]>>(
    next: &SimEvent,
    sq: &mut SimQueue,
    state: &SimState<M, RngSource>,
    recipient: &mut SimState<M, RngSource>,
    network: &mut ExtendedNetwork,
    current_time: &Instant,
) -> bool {
    let side = if next.client { "client" } else { "server" };

    match next.event {
        // here we simulate sending the packet into the tunnel
        TriggerEvent::NormalSent => {
            let mut send_time = next.time;
            // Apply blocking to packets going from client, or server to client
            if next.client || (!next.webserver && !next.tofrom_webserver)  {
                send_time = state.blocking_until.unwrap_or(next.time).max(next.time);
            }
            debug!("\tenqueue {:#?} Time: {:#?}", TriggerEvent::TunnelSent, send_time - *current_time);
            sq.push_sim(SimEvent {
                event: TriggerEvent::TunnelSent,
                time: send_time,
                integration_delay: next.integration_delay,
                client: next.client,
                webserver: next.webserver,
                tofrom_webserver: next.tofrom_webserver,
                packet_idx: next.packet_idx,
                contains_padding: false,
                bypass: false,
                replace: false,
                debug_note: None,
            });
            false
        }
        // here we simulate sending the packet into the tunnel
        TriggerEvent::PaddingSent { .. } => {
            if next.replace {
                // replace flag is set: if we have a normal packet queued up /
                // blocked, we can replace the padding with that FIXME: here be
                // bugs related to integration delays
                debug!("\tpadding replace @{}", side);
                //                if let (Some(queued), qid) =
                //                    sq.peek_blocking(state.blocking_bypassable, next.client)
                //
                if state.blocking_bypassable {
                    let queued = match next.client {
                        true => sq.client.blocking.peek(),
                        false => sq.server.blocking.peek(),
                    };
                    if queued.is_some() {
                        let queued = queued.unwrap();
                        debug!(
                            "\tQueued normal packet found for possible padding replacement @{}",
                            side
                        );

                        if (queued.client == next.client
                            // If at server, only do when server is sending to client
                            && !next.tofrom_webserver)
                            && TriggerEvent::TunnelSent == queued.event
                            && !queued.contains_padding
                        {
                            // two options:
                            // 1. the padding has the bypass flag set, so we need to
                            //    propagate the flag to the queued packet
                            // 2. the bypass flag is not set, which is also the case
                            //    for normal packets, so we do nothing
                            if !next.bypass {
                                debug!(
                                    "\tNormal packet was available, but not replacing padding as bypass==false @{}",
                                    side
                                );
                                return false;
                            }

                            // TODO:clean up below, simplify peeking and popping. Are both bypassable & blocking
                            // queues actually needed?
                            let (_queued2, qid) =
                                sq.peek_blocking(state.blocking_bypassable, next.client, next.webserver);     
                            debug!("padding q peeked: {:?}", qid);

                            // we need to remove and re-insert to get the packet
                            // into the correct internal queue with the new flags
                            let mut entry = sq
                                .pop_blocking(
                                    qid,
                                    state.blocking_bypassable,
                                    next.client,
                                    next.webserver,
                                )
                                .unwrap();
                            entry.bypass = true;
                            entry.replace = false;
                            // Since
                            entry.time = *current_time;
                            debug!(
                                "\treplaced bypassable padding sent with blocked queued normal TunnelSent @{}",
                                side
                            );

                            sq.push_sim(entry);
                            return false;
                        }
                    }
                }
            }
            // nothing to replace with (or we're not replacing), so queue up
            debug!("\tqueue {:#?}", TriggerEvent::TunnelSent);
            debug!("\tqueue padding sent @{}", side);
            let mut send_time = next.time;

            // Check if padding can bypass blocking, if not send when blocking ends
            if state.blocking_until.is_some() && !(state.blocking_bypassable && next.bypass) {
                send_time = max(state.blocking_until.expect("bug"), next.time);
            }

            if let Some(blocking_until) = state.blocking_until {
                debug!(
                    "\tqueue padding: send time {:?}   blocking until {:?}  state.bypassable {:?}",
                    send_time, blocking_until, state.blocking_bypassable
                );
            } else {
                debug!(
                    "\tqueue padding: send time {:?}   blocking until <None>  state.bypassable {:?}",
                    send_time,
                    state.blocking_bypassable
                );
            }
            sq.push_sim(SimEvent {
                event: TriggerEvent::TunnelSent,
                //time: next.time,
                time: send_time,
                integration_delay: next.integration_delay,
                client: next.client,
                webserver: false,
                tofrom_webserver: next.tofrom_webserver,
                packet_idx: usize::MAX,
                contains_padding: true,
                bypass: next.bypass,
                replace: next.replace,
                debug_note: None,
            });
            false
        }

        TriggerEvent::NormalRecv => {
            // Since we now have server as "router" between client and webserver,
            // we need to route packets through at server
            if !next.client && !next.webserver {
                debug!("\tqueue {:#?} Server Routing", TriggerEvent::NormalRecv);
                sq.push_sim(SimEvent {
                    event: TriggerEvent::NormalSent,
                    time: next.time,
                    integration_delay: next.integration_delay,
                    client: false,
                    webserver: false,
                    tofrom_webserver: !next.tofrom_webserver,
                    packet_idx: next.packet_idx,
                    contains_padding: false,
                    bypass: false,
                    replace: false,
                    debug_note: None,
                });
            } else {
                // We are at client or webserver, and should check if we have received a
                // packet_idx which has dependent packets that should be transmitted
                debug!("\tqueue {:#?} tx_depend check", TriggerEvent::NormalRecv);
                let dependent_events = sq.dependent_tx.get(&next.packet_idx);
                if dependent_events.is_some() {
                    // We have dependent packets, so we need to queue them up. Apply cloning for now, unoptimized
                    for (new_pktidx, delta, event_kind) in dependent_events.unwrap().clone() {
                        debug!("\tqueue tx_depend new_idx: {:#?}   delta: {:#?}   kind: {:#?} ", new_pktidx, delta, event_kind);
                        if event_kind == EventKind::CliSend {
                            // We are at client, and we toward webserver
                            sq.push_sim(SimEvent {
                                event: TriggerEvent::NormalSent,
                                time: *current_time + Duration::from_micros(delta as u64),
                                integration_delay: next.integration_delay,
                                client: true,
                                webserver: false,
                                tofrom_webserver: false,
                                packet_idx: new_pktidx,
                                contains_padding: false,
                                bypass: false,
                                replace: false,
                                debug_note: None,
                            });
                        } else {
                            // We are at webserver, and we send towards client
                            sq.push_sim(SimEvent {
                                event: TriggerEvent::NormalSent,
                                time: *current_time + Duration::from_micros(delta as u64),
                                integration_delay: next.integration_delay,
                                client: false,
                                webserver: true,
                                tofrom_webserver: false,
                                packet_idx: new_pktidx,
                                contains_padding: false,
                                bypass: false,
                                replace: false,
                                debug_note: None,
                            });
                        }
                    }
                }
            }
            false
        }



        TriggerEvent::TunnelSent => {
            let reporting_delay = recipient.reporting_delay();
            let (network_delay, _baseline_delay) = network.sample(current_time, next.client);

            // We now have client, server, webserver, so need to set bools correctly
            let (next_client, next_webserver, next_tofrom_webserver) =
                match (next.client, next.webserver, next.tofrom_webserver) {
                    // From client to server
                    (true, false, false) => (false, false, false),
                    // From server to client
                    (false, false, false) => (true, false, false),
                    // From server to webserver
                    (false, false, true) => (false, true, false),
                    // From webserver to server
                    (false, true, false) => (false, false, true),
                    _ => panic!(
                    "Invalid client/webserver/tofrom_webserver combination for TunnelSent event"
                ),
                };

            if !next.contains_padding {
                // The time the event was reported to us is in next.time. We have to
                // remove the reporting delay locally, then add a network delay and
                // a reporting delay (at the recipient) for the recipient.
                //
                // LIMITATION, we also have to deal with an ugly edge-case: if the
                // reporting delay is very long *at the sender*, then the event can
                // actually arrive earlier at the recipient than it was reported to
                // the sender. This we cannot deal with in the current design of the
                // simulator (support for integration delays was bolted on late),
                // because it would move time backwards. Therefore, we clamp.

                let reported = max(
                    next.time - next.integration_delay + network_delay + reporting_delay,
                    *current_time,
                );
                sq.push_sim(SimEvent {
                    event: TriggerEvent::TunnelRecv,
                    time: reported,
                    integration_delay: reporting_delay,
                    client: next_client,
                    webserver: next_webserver,
                    tofrom_webserver: next_tofrom_webserver,
                    packet_idx: next.packet_idx,
                    contains_padding: false,
                    bypass: false,
                    replace: false,
                    debug_note: None,
                });
                debug!(
                    "\tqueue {:#?}, arriving at recipient in {:?}",
                    TriggerEvent::TunnelRecv,
                    reported - *current_time
                );
                return true;
            }

            // padding, less complicated: action delay + network + recipient
            // reporting delay
            let reported = next.time + next.integration_delay + network_delay + reporting_delay;
            sq.push_sim(SimEvent {
                event: TriggerEvent::TunnelRecv,
                time: reported,
                integration_delay: reporting_delay,
                client: next_client,
                webserver: next_webserver,
                tofrom_webserver: next_tofrom_webserver,
                packet_idx: usize::MAX,
                contains_padding: true,
                bypass: false,
                replace: false,
                debug_note: None,
            });
            debug!(
                "\tqueue {:#?}, arriving at recipient in {:?}",
                TriggerEvent::TunnelRecv,
                reported - *current_time
            );
            true
        }
        TriggerEvent::TunnelRecv => {
            // spawn NormalRecv or PaddingRecv
            if next.contains_padding {
                debug!("\tqueue {:#?}", TriggerEvent::PaddingRecv);
                sq.push(
                    TriggerEvent::PaddingRecv,
                    next.client,
                    next.webserver,
                    false,
                    usize::MAX,
                    true,
                    next.time,
                    next.integration_delay,
                );
            } else {
                debug!("\tqueue {:#?}", TriggerEvent::NormalRecv);
                sq.push(
                    TriggerEvent::NormalRecv,
                    next.client,
                    next.webserver,
                    next.tofrom_webserver,
                    next.packet_idx,
                    false,
                    next.time,
                    next.integration_delay,
                );
            }
            true
        }
        // all other events are not network activity
        _ => false,
    }
}

pub struct SimulEvent {
    /// the actual event
    pub event: TriggerEvent,
    /// the time of the event taking place
    pub time: Instant,
    /// Packet ID for triggering dependent tx events
    pub packet_idx: usize,
    /// Node index and link index for the event
    pub node_idx: usize,
    pub link_idx: usize,
    /// flag to track padding or normal packet
    pub contains_padding: bool,
    /// internal flag to mark event as bypass
    bypass: bool,
    /// internal flag to mark event as replace
    replace: bool,
    // debug note
    pub debug_note: Option<String>,
}
