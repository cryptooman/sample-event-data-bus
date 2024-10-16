package main

import (
    "fmt"
    "log"
    "time"
    "strconv"
    "os"
    "math/rand"
    "sync"
    "sync/atomic"
)

const (
    EventBusSize        = 256           // Event data bus size
    MessageSizeMax      = 16            // Event message max length
    PublishQueueLen     = 4<<10         // Max messages in a publish queue per Producer
    EventBusWriteDelay  = true          // Emulate storage write acknowledged delay
    
    Producers           = 10            // Producers count
    ProducerMessages    = 1000000       // Amount of messages per Producer
    Consumers           = 10            // Consumers count
    Verbose             = false
    // OR
    //Producers           = 1
    //ProducerMessages    = 10000000
    //Consumers           = 1
    //Verbose             = false
    // OR
    //Producers           = 2
    //ProducerMessages    = 2
    //Consumers           = 2
    //Verbose             = true
)

type EventBus struct {
    Data EventBusData
    publishQueues EventBusPublishQueues // Published messages ready to be read
}

type EventBusPublishQueues map[int]*chan EventBusDataKey

type EventBusData map[EventBusDataKey]*EventBusDataValue

type EventBusDataKey uint32

type EventBusDataValue struct {
    Messages []EventBusDataValueMessage
    offsetRead int64
    mu sync.RWMutex
}

type EventBusDataValueMessage [MessageSizeMax]byte 

func (b *EventBus) Key(msg string) EventBusDataKey {
    s := 0
    for i := 0; i < len(msg); i++ {
        s += int(msg[i])
    }
    return EventBusDataKey(s % EventBusSize)
}

func main() {
    
    if Producers != Consumers { // For simplicity
        log.Fatalln("Producers must be equal to Consumers")
    }
    
    // Init event data bus
    
    bus := &EventBus{Data: make(EventBusData, EventBusSize)}
    for bk := EventBusDataKey(0); bk < EventBusDataKey(EventBusSize); bk++ {
        bus.Data[bk] = &EventBusDataValue{
            Messages: make([]EventBusDataValueMessage, 0, PublishQueueLen),
        }
    }
    
    bus.publishQueues = make(EventBusPublishQueues, Producers)
    for p := 0; p < Producers; p++ {
        ch := make(chan EventBusDataKey, PublishQueueLen)
        bus.publishQueues[p] = &ch
    }
    fmt.Printf("Event data bus inited: size: %v\n", EventBusSize)
    
    // Generate messages

    rand.Seed(time.Now().UTC().UnixNano())
    messages := make([][]string, Producers)
    for p := 0; p < Producers; p++ {
        messages[p] = make([]string, ProducerMessages)
        for m := 0; m < ProducerMessages; m++ {
            // Message to publish
            messages[p][m] = "m_" +
                strconv.Itoa(rand.Intn(1000)) + "_" + 
                strconv.Itoa(rand.Intn(1000)) + "_" + 
                strconv.Itoa(rand.Intn(1000))
        }
    }
    fmt.Printf("Generated messages: total: %v\n", Producers * ProducerMessages)
    
    // Calculate messages bus keys (~ message hash)
    
    messagesKeys := make([][]EventBusDataKey, Producers)
    for p := 0; p < Producers; p++ {
        messagesKeys[p] = make([]EventBusDataKey, ProducerMessages)
        for m := 0; m < ProducerMessages; m++ {
            messagesKeys[p][m] = bus.Key(messages[p][m])
        }
    }
    fmt.Printf("Calculated messages bus keys\n")
    
    // Run
    
    fmt.Printf("Producers: %v\n", Producers)
    fmt.Printf("Consumers: %v\n", Consumers)
    
    fmt.Printf("Sending messages ...\n")
    
    timeStart := time.Now()
    
    // Producers
    
    for p := 0; p < Producers; p++ {
        p := p // for compatibility with older versions of golang
        go func() {
            for m := 0; m < ProducerMessages; m++ {

                msg := messages[p][m] // Message to publish
                bk := messagesKeys[p][m] // Message bus key
                
                bus.Data[bk].mu.Lock()
                
                var mbyt EventBusDataValueMessage
                copy(mbyt[:], msg)
                bus.Data[bk].Messages = append(bus.Data[bk].Messages, mbyt)
                
                if EventBusWriteDelay {
                    time.Sleep(time.Microsecond * 1)
                }
                
                if Verbose {
                    fmt.Printf("producer[%v][%v] bk[%v]:\t%v\n", p, m, bk, msg)
                }
                bus.Data[bk].mu.Unlock()
                
                *bus.publishQueues[p] <- bk
            }
            close(*bus.publishQueues[p])
        }()
    }
    
    // Consumers
    
    var wgConsumers sync.WaitGroup
    wgConsumers.Add(Consumers)
    
    for c := 0; c < Consumers; c++ {
        c := c // for compatibility with older versions of golang
        go func() {
            defer wgConsumers.Done()
            for bk := range *bus.publishQueues[c] {
                bus.Data[bk].mu.RLock()
                if Verbose {
                    fmt.Printf("consumer[%v][-] bk[%v]:\t%v\n", c, bk, 
                               string(bus.Data[bk].Messages[ bus.Data[bk].offsetRead ][:]))
                }
                atomic.AddInt64(&bus.Data[bk].offsetRead, 1)
                bus.Data[bk].mu.RUnlock()
            }
        }()
    }
    
    wgConsumers.Wait()
    
    timeTaken := time.Since(timeStart).Milliseconds()
    fmt.Printf("Time taken: %v ms\n", timeTaken)
    os.Exit(0)
}
