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
  BlockingQueueResponse response;
  BlockingQueue *queue;
  OperationType operationType;
  int data;
  int isComplete;
  int threadIndex;
} BlockingQueueRequest;

void initializeGlobals(int threadCount);

BlockingQueueRequest *getActiveRequest(int index);
int setActiveRequest(BlockingQueueRequest *request, int index);
pthread_t *getActiveThread(int index);
int setActiveThread(pthread_t *thread, int index);
void printActiveRequests();
BlockingQueue *createBlockingQueue(int capacity);
BlockingQueue *destroyBlockingQueue(BlockingQueue *ptr);
BlockingQueueRequest *createBlockingQueueRequest(BlockingQueue *q, OperationType op, int data);
BlockingQueueRequest *destroyBlockingQueueRequest(BlockingQueueRequest *ptr);
int validateRequest(BlockingQueueRequest *request);
void *enqueue_implementation(void *input);
void *dequeue_implementation(void *input);
void scheduleRequest(BlockingQueueRequest *request);
void enqueue(BlockingQueueRequest *request);
void dequeue(BlockingQueueRequest *request);

int THREAD_COUNT = -1;
BlockingQueueRequest **ACTIVE_REQUESTS = NULL;
pthread_t **ACTIVE_THREADS = NULL;

void initializeGlobals(int threadCount) {
  int i;

  THREAD_COUNT = threadCount;
  ACTIVE_REQUESTS = malloc(sizeof(BlockingQueueRequest*) * threadCount);

  if (ACTIVE_REQUESTS == NULL) {
    return;
  }

  ACTIVE_THREADS = malloc(sizeof(pthread_t*) * threadCount);

  if (ACTIVE_THREADS == NULL) {
    free(ACTIVE_REQUESTS);
    return;
  }

  for (i = 0; i < threadCount; i++) {
    ACTIVE_THREADS[i] = malloc(sizeof(pthread_t));
    ACTIVE_REQUESTS[i] =  NULL;
  }

  return;
}

BlockingQueueRequest *getActiveRequest(int index) {
  if (ACTIVE_REQUESTS == NULL)
    return NULL;
  else if (index == -1 || index >= THREAD_COUNT)
    return NULL;
  else
    return ACTIVE_REQUESTS[index];
}

int setActiveRequest(BlockingQueueRequest *request, int index) {
  if (ACTIVE_REQUESTS == NULL)
    return 0;
  else if (index == -1 || index >= THREAD_COUNT) {
    return 0;
  }
  else {
    ACTIVE_REQUESTS[index] = request;
    return 1;
  }
}

pthread_t *getActiveThread(int index) {
  if (ACTIVE_THREADS == NULL)
    return NULL;
  else if (index == -1 || index >= THREAD_COUNT)
    return NULL;
  else
    return ACTIVE_THREADS[index];
}

int setActiveThread(pthread_t *thread, int index) {
  if (ACTIVE_THREADS == NULL)
    return 0;
  else if (index == -1 || index >= THREAD_COUNT) {
    return 0;
  }
  else {
    ACTIVE_THREADS[index] = thread;
    return 1;
  }
}

void printActiveRequests() {
  int i;

  // printf("Active Requests: [");

  for (i = 0; i < THREAD_COUNT; i++) {
    printf("%p, ", ACTIVE_REQUESTS[i]);
  }

  printf("]\n\n");

}

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
  request->isComplete = 0;

  request->response.errorType = NO_ERROR;
  request->response.hasError = 0;
  request->response.data = INT_MIN;

  return request;
}

BlockingQueueRequest *destroyBlockingQueueRequest(BlockingQueueRequest *ptr) {
  if (ptr == NULL)
    return NULL;

  free(ptr);

  return NULL;
}

int validateRequest(BlockingQueueRequest *request) {

  if (request == NULL) {
    request->response.hasError = 1;
    request->response.errorType = REQUEST_NULL_EXCEPTION;
    return 0;
  }
  else if (request->queue == NULL || request->queue->array == NULL) {
    request->response.hasError = 1;
    request->response.errorType = QUEUE_NULL_EXCEPTION;
    return 0;
  }

  return 1;
}

void *enqueue_implementation(void *input) {
  BlockingQueueRequest *request = (BlockingQueueRequest*)input;
  BlockingQueue *queue = NULL;

  // there was an error, cancel here
  if (!validateRequest(request)) {
    request->isComplete = 1;
    return request;
  }

  queue = request->queue;

  // lock queue
  pthread_mutex_lock(&queue->headLock);

  // queue is full, unlock and return
  if (queue->tail - queue->head == queue->capacity) {
    if (DEBUG > 1) {
      printf("- [%c] enqueue failed, queue full\n", request->threadIndex + 'A');
    }

    request->response.hasError = 1;
    request->response.errorType = QUEUE_FULL_EXCEPTION;
    pthread_mutex_unlock(&queue->headLock);
    request->isComplete = 1;

    return request;
  }

  queue->array[queue->tail % queue->capacity] = request->data;
  queue->tail++;

  if (DEBUG > 1) {
    printf("- [%c] enqueued %d\n", request->threadIndex + 'A', request->data);
  }

  pthread_mutex_unlock(&queue->headLock);
  request->isComplete = 1;

  return request;
}

// removes an element from the queue and returns it
void *dequeue_implementation(void *input) {
  BlockingQueueRequest *request = (BlockingQueueRequest*)input;
  BlockingQueue *queue = NULL;
  int data;

  if (!validateRequest(request)) {
    request->isComplete = 1;
    return request;
  }

  queue = request->queue;

  pthread_mutex_lock(&queue->headLock);

  if (queue->tail == queue->head) {
    if (DEBUG > 1)
      printf("- [%c] deqeue failed, queue empty\n", request->threadIndex + 'A');

    request->response.hasError = 1;
    request->response.errorType = QUEUE_EMPTY_EXCEPTION;
    pthread_mutex_unlock(&queue->headLock);

    request->isComplete = 1;
    return request;
  }

  request->response.data = queue->array[queue->head % queue->capacity];
  queue->head++;
  pthread_mutex_unlock(&queue->headLock);

  if (DEBUG > 1) {
    printf("- [%c] dequeued %d.\n", request->threadIndex + 'A', request->response.data);
  }

  request->isComplete = 1;

  return request;
}

void scheduleRequest(BlockingQueueRequest *request) {
  int  threadIndex = 0;
  BlockingQueueRequest *current = NULL;

  // while there's no thread available
  while (1) {
    current = getActiveRequest(threadIndex % THREAD_COUNT);

    if (current == NULL) {
      break;
    }
    else if(current->isComplete) {
      // freeing this should be done in main after program is finished running
      setActiveRequest(NULL, threadIndex % THREAD_COUNT);

      // wait for the thread to say it's done
      pthread_join(*getActiveThread(threadIndex % THREAD_COUNT), NULL);

      break;
    }

    threadIndex++;
  }

  request->threadIndex = threadIndex % THREAD_COUNT;
  setActiveRequest(request, threadIndex % THREAD_COUNT);

  if (request->operationType == ENQUEUE) {
    pthread_create(getActiveThread(threadIndex % THREAD_COUNT), NULL, enqueue_implementation, (void*)request);
  }
  else {
    pthread_create(getActiveThread(threadIndex % THREAD_COUNT), NULL, dequeue_implementation, (void*)request);
  }

}

void enqueue(BlockingQueueRequest *request) {
  scheduleRequest(request);
  return;
}

void dequeue(BlockingQueueRequest *request) {
  scheduleRequest(request);
  return;
}

int main(int argc, char **argv) {
  int i, r, threadCount, operationLimit, operationCount;
  BlockingQueue *queue = NULL;
  BlockingQueueRequest **requestList;

  if (argc < 3) {
    printf("Invalid program input.  Please run using ./a.out <threadCount> <operationCount>\n\n");
    return -1;
  }

  srand(time(NULL));

  threadCount = atoi(argv[1]);
  operationLimit = atoi(argv[2]);

  initializeGlobals(threadCount);

  queue = createBlockingQueue(10); 
  
  requestList = malloc(sizeof(BlockingQueueRequest*) * operationLimit);

  if (DEBUG > 2)
    printf("request list: \n[");

  // generate operations and elements that can be enqueued
  for (i = 0; i < operationLimit; i++) {
    r = rand() % 1000;
    requestList[i] = createBlockingQueueRequest(queue, r % 2 ? ENQUEUE : DEQUEUE, r);

    if (DEBUG > 2) {
      if (requestList[i]->operationType == ENQUEUE)
        printf("enq(%d), ", requestList[i]->data);
      else 
        printf("deq(), ");
    }
  }

  if (DEBUG > 2) 
    printf("]\n\n");

  operationCount = 0;

  while (operationCount < operationLimit) {
    if (requestList[operationCount]->operationType == ENQUEUE) {
      enqueue(requestList[operationCount++]);
    }
    else {
      dequeue(requestList[operationCount++]);
    }
  }

  // wait for remaining active requests to finish
  for (i = 0; i < THREAD_COUNT; i++) {
    if (getActiveThread(i) != NULL)
      pthread_join(*getActiveThread(i), NULL);
  }

  return 0;
}

