// btleplug Source Code File
//
// Copyright 2020 Nonpolynomial. All rights reserved.
//
// Licensed under the BSD 3-Clause license. See LICENSE file in the project root
// for full license information.

use crate::api::ValueNotification;
use futures::stream::{Stream, StreamExt};
use log::warn;
use std::pin::Pin;
use tokio::sync::broadcast::Receiver;
use tokio_stream::wrappers::{BroadcastStream, errors::BroadcastStreamRecvError};

pub const NOTIFICATIONS_CHANNEL_CAPACITY: usize = 4096;

pub fn notifications_stream_from_broadcast_receiver(
    receiver: Receiver<ValueNotification>,
) -> Pin<Box<dyn Stream<Item = ValueNotification> + Send>> {
    Box::pin(BroadcastStream::new(receiver).filter_map(|x| async move {
        match x {
            Ok(notification) => Some(notification),
            Err(BroadcastStreamRecvError::Lagged(count)) => {
                warn!("btleplug notification receiver lagged; dropped {count} notification(s)");
                None
            }
        }
    }))
}
