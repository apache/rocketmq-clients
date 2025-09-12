package org.apache.rocketmq.client.java.impl.consumer;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.java.exception.LiteSubscriptionQuotaExceededException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class LitePushConsumerImplTest {

    @Mock
    private LitePushConsumerSettings mockSettings;

    private LitePushConsumerImpl consumer;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        consumer = mock(LitePushConsumerImpl.class, CALLS_REAL_METHODS);
        // Set final field litePushConsumerSettings using reflection
        try {
            java.lang.reflect.Field field = LitePushConsumerImpl.class.getDeclaredField("litePushConsumerSettings");
            field.setAccessible(true);
            field.set(consumer, mockSettings);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testSubscribeLite_NotRunning() throws ClientException {
        String liteTopic = "testLiteTopic";
        doThrow(new IllegalStateException("not running")).when(consumer).checkRunning();

        consumer.subscribeLite(liteTopic);
    }

    @Test
    public void testSubscribeLite_AlreadySubscribed() throws ClientException {
        String liteTopic = "testLiteTopic";
        doNothing().when(consumer).checkRunning();
        when(mockSettings.containsLiteTopic(liteTopic)).thenReturn(true);

        consumer.subscribeLite(liteTopic);

        verify(consumer).checkRunning();
        verify(mockSettings).containsLiteTopic(liteTopic);
        verify(consumer, never()).syncLiteSubscription(any(), any());
    }

    @Test(expected = LiteSubscriptionQuotaExceededException.class)
    public void testSubscribeLite_QuotaExceeded() throws ClientException {
        String liteTopic = "testLiteTopic";
        doNothing().when(consumer).checkRunning();
        when(mockSettings.containsLiteTopic(liteTopic)).thenReturn(false);
        when(mockSettings.getMaxLiteTopicSize()).thenReturn(128);
        when(mockSettings.getLiteSubscriptionQuota()).thenReturn(10);
        when(mockSettings.getLiteTopicSetSize()).thenReturn(10); // Quota full

        consumer.subscribeLite(liteTopic);
    }
}
