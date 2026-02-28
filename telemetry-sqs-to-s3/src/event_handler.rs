use lambda_runtime::{tracing, Error, LambdaEvent};
use aws_lambda_events::event::sqs::SqsEvent;

/// This is the main body for the function.
/// Write your code inside it.
/// There are some code example in the following URLs:
/// - https://github.com/awslabs/aws-lambda-rust-runtime/tree/main/examples
/// - https://github.com/aws-samples/serverless-rust-demo/
pub(crate)async fn function_handler(event: LambdaEvent<SqsEvent>) -> Result<(), Error> {
    // Extract some useful information from the request
    let payload = event.payload;
    tracing::info!("Payload: {:?}", payload);

    payload.records.iter().for_each(|r| {
        tracing::info!("Record body: {:?}", r.body);
        // Parse body as JSON
        let body = r.body.as_deref().unwrap_or(""); 

        if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(&body) {
            tracing::info!("Parsed JSON: {:?}", json_value.as_object().unwrap());
        } else {
            tracing::warn!("Failed to parse body as JSON");
        }
    });

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use lambda_runtime::{Context, LambdaEvent};

    #[tokio::test]
    async fn test_event_handler() {
        let event = LambdaEvent::new(SqsEvent::default(), Context::default());
        let response = function_handler(event).await.unwrap();
        assert_eq!((), response);
    }
}
