using Apache.Rocketmq.V2;
using Google.Protobuf;

namespace Org.Apache.Rocketmq.Error
{
    public class StatusChecker
    {
        public static void check(Status status, IMessage message)
        {
            // var code = status.Code;
            // switch (code)
            // {
            //     case Code.Ok:
            //     case Code.MultipleResults:
            //         return;
            //     case Code.BadRequest:
            //     case Code.IllegalAccessPoint:
            //     case Code.IllegalTopic:
            //     case Code.IllegalConsumerGroup:
            //     case Code.IllegalMessageTag:
            //     case Code.IllegalMessageKey:
            //     case Code.IllegalMessageGroup:
            //     case Code.IllegalMessagePropertyKey:
            //     case Code.InvalidTransactionId:
            //     case Code.IllegalMessageId:
            //     case Code.IllegalFilterExpression:
            //     case Code.IllegalInvisibleTime:
            //     case Code.IllegalDeliveryTime:
            //     case Code.InvalidReceiptHandle:
            //     case Code.MessagePropertyConflictWithType:
            //     case Code.UnrecognizedClientType:
            //     case Code.MessageCorrupted:
            //     case Code.ClientIdRequired:
            //     case Code.IllegalPollingTime:
            //         throw new BadRequestException(code)
            //
            //     case ILLEGAL_POLLING_TIME:
            // }
        }
    }
}