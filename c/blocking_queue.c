#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <pthread.h>
#include <time.h>

#define DEBUG 5

typedef enum OperationType {
  ENQUEUE,
  DEQUEUE
} OperationType;

typedef enum ErrorType {
  QUEUE_FULL_EXCEPTION,
  QUEUE_EMPTY_EXCEPTION,
  QUEUE_NULL_EXCEPTION,
  REQUEST_NULL_EXCEPTION,
  NO_ERROR
} ErrorType;

typedef struct BlockingQueue {
  int *array;
  int capacity;
  int head, tail;
  pthread_mutex_t headLock, tailLock;
} BlockingQueue;

typedef struct BlockingQueueResponse {
  ErrorType errorType;
  int hasError;
  int data;
} BlockingQueueResponse;

typedef struct BlockingQueueRequest {
  BlockingQueueResponse *response;
  BlockingQueue *queue;
  OperationType operationType;
  int data;
} BlockingQueueRequest;

BlockingQueue *createBlockingQueue(int capacity);
BlockingQueue *destroyBlockingQueue(BlockingQueue *ptr);
BlockingQueueResponse *createBlockingQueueResponse(ErrorType errorType, int hasError, int data);
BlockingQueueRequest *createBlockingQueueRequest(BlockingQueue *q, OperationType op, int data);
BlockingQueueRequest *destroyBlockingQueueRequest(BlockingQueueRequest *ptr);
int validateRequest(BlockingQueueRequest *request);
void *enqueue(void *input);
void *dequeue(void *input);


// allocates memory for a new BlockingQueue
BlockingQueue *createBlockingQueue(int capacity) {
  BlockingQueue *ptr = NULL;

  ptr = malloc(sizeof(BlockingQueue));

  if (ptr == NULL) {
    return NULL;
  }

  ptr->array = malloc(sizeof(capacity));

  if (ptr->array == NULL) {
    free(ptr);
    return NULL;
  }

  ptr->capacity = capacity;
  ptr->head = 0;
  ptr->tail = 0;

  pthread_mutex_init(&ptr->headLock, NULL);

  return ptr;
}

// frees up memory for a BlockingQueue
BlockingQueue *destroyBlockingQueue(BlockingQueue *ptr) {
  if (ptr == NULL)
    return NULL;

  pthread_mutex_destroy(&ptr->headLock);

  if (ptr->array != NULL)
    free(ptr->array);

  free(ptr);

  return NULL;
}

BlockingQueueResponse *createBlockingQueueResponse(ErrorType errorType, int hasError, int data) {

  BlockingQueueResponse *response = NULL;

  response = malloc(sizeof(BlockingQueueResponse));

  if (response == NULL)
    return NULL;

  response->errorType = errorType;
  response->hasError = hasError;
  response->data = data;

  return response;
}

BlockingQueueRequest *createBlockingQueueRequest(BlockingQueue *q, OperationType op, int data) {
  BlockingQueueRequest *request = NULL;

  if (q == NULL)
    return NULL;

  request = malloc(sizeof(BlockingQueueRequest));

  if (request == NULL)
    return NULL;

  request->queue = q;
  request->operationType = op;
  request->data = data;
  request->response = createBlockingQueueResponse(NO_ERROR, 0, INT_MIN);

  return request;
}

BlockingQueueRequest *destroyBlockingQueueRequest(BlockingQueueRequest *ptr) {
  if (ptr == NULL)
    return NULL;

  if (ptr->response != NULL)
    free(ptr->response);

  free(ptr);

  return NULL;
}

int validateRequest(BlockingQueueRequest *request) {

  if (request == NULL || request->response == NULL) {
    return 0;
  }
  else if (request == NULL) {
    request->response->hasError = 1;
    request->response->errorType = REQUEST_NULL_EXCEPTION;
    return 0;
  }
  else if (request->queue == NULL || request->queue->array == NULL) {
    request->response->hasError = 1;
    request->response->errorType = QUEUE_NULL_EXCEPTION;
    return 0;
  }

  return 1;
}

void *enqueue(void *input) {
  BlockingQueueRequest *request = (BlockingQueueRequest*)input;
  BlockingQueue *queue = NULL;

  // there was an error, cancel here
  if (!validateRequest(request)) {
    return request;
  }

  queue = request->queue;

  // lock queue
  pthread_mutex_lock(&queue->headLock);

  // queue is full, unlock and return
  if (queue->tail - queue->head == queue->capacity) {
    request->response->hasError = 1;
    request->response->errorType = QUEUE_FULL_EXCEPTION;

    pthread_mutex_unlock(&queue->headLock);
    return request->response;
  }

  if (DEBUG > 2) {
    printf("- enqueueing element %d at position %d.\n", request->data, queue->tail);
  }
  
  queue->array[queue->tail % queue->capacity] = request->data;
  queue->tail++;

  // unlock
  pthread_mutex_unlock(&queue->headLock);

  return request;
}

// removes an element from the queue and returns it
void *dequeue(void *input) {
  BlockingQueueRequest *request = (BlockingQueueRequest*)input;
  BlockingQueue *queue = NULL;
  int data;

  if (!validateRequest(request)) {
    return request;
  }

  queue = request->queue;

  pthread_mutex_lock(&queue->headLock);

  if (queue->tail == queue->head) {
    request->response->hasError = 1;
    request->response->errorType = QUEUE_EMPTY_EXCEPTION;

    pthread_mutex_unlock(&queue->headLock);
    return request;
  }

  if (DEBUG > 2) {
    printf("- dequeueing element at position %d.\n", queue->head);
  }

  request->response->data = queue->array[queue->head % queue->capacity];
  queue->head++;
  
  pthread_mutex_unlock(&queue->headLock);

  return request;
}

int main(int argc, char **argv) {
  BlockingQueue *queue = NULL; 
  BlockingQueueRequest **requests = NULL;
  pthread_t **threadArray = NULL;
  OperationType operationType;
  int i, threadCount, operationLimit, operationCount, enqueueCount;
  int *operationList, *enqueueList;

  srand(time(NULL));

  if (argc < 3) {
    printf("Invalid program input.  Please run using ./a.out <threadCount> <operationCount>\n\n");
    return -1;
  }

  threadCount = atoi(argv[1]);
  operationLimit = atoi(argv[2]);

  queue = createBlockingQueue(10); 

  threadArray = malloc(sizeof(pthread_t*) * threadCount);
  requests = malloc(sizeof(BlockingQueueRequest*) * threadCount);

  operationList = malloc(sizeof(int) * operationLimit);
  enqueueList = malloc(sizeof(int) * operationLimit);

  // generate operations and elements that can be enqueued
  for (i = 0; i < operationLimit; i++) {
    enqueueList[i] = rand() % 1000;
    operationList[i] = enqueueList[i] % 2;
  }

  // intiializes thread
  for (i = 0; i < threadCount; i++) {
    threadArray[i] = malloc(sizeof(pthread_t));
  }

  // executes generated operations with generated values
  operationCount = enqueueCount = 0;
  while (operationCount < operationLimit) {
    // 1: enqueue, 0: dequeue
    operationType = operationList[operationCount] ? ENQUEUE : DEQUEUE;

    // @TODO: need to figure out how to manage threads w/ threadpool
    // so that they can be reused once other threads are finished executing
    // can current only have operationLimit < threadCount
    if (operationType == ENQUEUE) {
      printf("enqueue %d\n", enqueueList[enqueueCount]);
      requests[operationCount] = createBlockingQueueRequest(queue, ENQUEUE, enqueueList[enqueueCount++]);
      pthread_create(threadArray[operationCount], NULL, enqueue, (void*)requests[operationCount]);
    }
    else {
      printf("dequeue\n");
      requests[operationCount] = createBlockingQueueRequest(queue, DEQUEUE, INT_MIN);
      pthread_create(threadArray[operationCount], NULL, dequeue, (void*)requests[operationCount]);
    }

    operationCount++;
  }

  // waits for all threads to finish
  for (i = 0; i < threadCount; i++)
    pthread_join(*threadArray[i], NULL);

  if (DEBUG > 2)
    printf("Finished creating enqueue threads.\n");

  return 0;
}

