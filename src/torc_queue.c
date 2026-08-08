/*
 *  torc_queue.c
 *  TORC_Lite
 *
 *  Created by Panagiotis Hadjidoukas on 1/1/14.
 *  Copyright 2014 ETH Zurich. All rights reserved.
 *
 */
#include <torc_internal.h>
#include <torc.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static pthread_mutex_t scheduling_lock = PTHREAD_MUTEX_INITIALIZER;
static int next_target_worker = 0;
static int target_worker_initialized = 0;

static pthread_mutex_t rq_target_lock = PTHREAD_MUTEX_INITIALIZER;
static int rq_next_target_node = 0;
static int rq_target_initialized = 0;

static pthread_mutex_t rq_end_node_lock = PTHREAD_MUTEX_INITIALIZER;
static int rq_end_next_node = 0;
static int rq_end_node_initialized = 0;


/* Initialization of ready queues */
void rq_init ()
{
    _queue_init(&reuse_q);
    _queue_init(&private_grq);
    for (int i = 0; i < 10 ; i++)
        _queue_init(&public_grq[i]);
}

/* Reuse of descriptors */
static void torc_to_i_reuseq_end (torc_t *rte)
{
    _enqueue_tail(&reuse_q, rte);
}

static torc_t *torc_i_reuseq_dequeue ()
{
    torc_t *rte = NULL;

    _dequeue(&reuse_q, &rte);

    return rte;
}

torc_t *_torc_get_reused_desc()
{
    torc_t *rte = NULL;
    char *ptr;
    static unsigned long offset = sizeof(_lock_t) /*+ 16*sizeof(void *)*/;
    static unsigned long torc_size = sizeof(torc_t);

    /*    rte = calloc(1, sizeof(torc_t));*/
    /*    return rte;*/

    rte = torc_i_reuseq_dequeue();
    if (rte != NULL) {
        ptr = (char *) rte;
        ptr += offset;
        memset(ptr, 0, torc_size - offset);
    }
    else {
        rte = calloc(1, sizeof(torc_t));
        if (rte == NULL)
            Error("failed to allocate task descriptor");
    }

    return rte;
}

void _torc_put_reused_desc(torc_t *rte)
{
    /*    _lock_destroy(&rte->lock);*/
    /*    free(rte);*/
    /*    return;*/

    torc_to_i_reuseq_end(rte);
}


/*********************************************************************/
/************************* Intra-node Queues *************************/
/*********************************************************************/

/*
 * Private global queue
 */

void torc_to_i_pq (torc_t *rte)
{
    _enqueue_head(&private_grq, rte);
}


void torc_to_i_pq_end (torc_t *rte)
{
    _enqueue_tail(&private_grq, rte);
}

torc_t *torc_i_pq_dequeue ()
{
    torc_t *rte = NULL;

    _dequeue(&private_grq, &rte);
    return rte;
}


/*
 * Public global queue
 */

void torc_to_i_rq (torc_t *rte)
{
    int lvl = (rte->level <= 1)? 0: rte->level-1;
    if (lvl >= 10) lvl = 9;
    _enqueue_head(&public_grq[lvl], rte);
}


void torc_to_i_rq_end (torc_t *rte)
{
    int lvl = (rte->level <= 1)? 0: rte->level-1;
    if (lvl >= 10) lvl = 9;
    _enqueue_tail(&public_grq[lvl], rte);
}


torc_t *torc_i_rq_dequeue (int lvl)
{
    torc_t *rte = NULL;

    _dequeue(&public_grq[lvl], &rte);
    return rte;
}


/*********************************************************************/
/************************* Inter-node Queues *************************/
/*********************************************************************/

/*
 * Public global queue
 */

static void read_arguments(torc_t *rte)
{
    for (int i = 0; i < rte->narg; i++)
        rte->temparg[i] = rte->localarg[i];
}

void torc_to_nrq (int target_node, torc_t *rte)
{
#if DBG
    printf("rte_to_lrq_2: target = %d, target_node = %d, target_queue = %s\n", -1, target_node, "inter-node gq");
#endif

    rte->insert_in_front = 1;
    rte->inter_node = 1;    /* alternatively, it can be set only when it goes outside */
    rte->target_queue = -1;
    if (torc_node_id() != target_node) {
#if DBG
        printf("enqueing remotely: rte = %p\n", (void *)rte);
#endif
        send_descriptor(target_node, rte, TORC_NORMAL_ENQUEUE);
        _torc_put_reused_desc(rte);
    }
    else {
#if DBG
        printf("enqueing locally: rte = %p\n", (void *)rte);
#endif
        read_arguments(rte);
        torc_to_i_rq(rte);
    }
}

void torc_to_nrq_end (int target_node, torc_t *rte)
{
#if DBG
    printf("rte_to_lrq_end_2: target = %d, target_node = %d, target_queue = %s\n", -1, target_node, "inter-node gq");
#endif

    rte->inter_node = 1;    /* alternatively, it can be set only when it goes outside */
    rte->target_queue = -1;
    if (torc_node_id() != target_node) {
#if DBG
        printf("enqueing remotely: rte = %p\n", (void *)rte);
#endif
        send_descriptor(target_node, rte, TORC_NORMAL_ENQUEUE);
        _torc_put_reused_desc(rte);
    }
    else {
#if DBG
        printf("enqueing locally: rte = %p\n", (void *)rte);
#endif
        read_arguments(rte);
        torc_to_i_rq_end(rte);
    }
}


/*
 * Public global queue - general version
 */

void torc_to_rq(torc_t *rte)
{
    int target_node;
    int total_nodes = torc_num_nodes();

    if (total_nodes <= 0)
        Error("invalid total node count");

    pthread_mutex_lock(&rq_target_lock);

    if (!rq_target_initialized) {
        rq_next_target_node = torc_node_id();
        rq_target_initialized = 1;
    }

    target_node = rq_next_target_node;
    rq_next_target_node =
        (rq_next_target_node + 1) % total_nodes;

    pthread_mutex_unlock(&rq_target_lock);

#if DBG
    printf("torc_to_rq: target_node = %d\n", target_node);
#endif

    rte->insert_in_front = 1;
    rte->insert_private = 0;
    rte->inter_node = 1;
    rte->target_queue = -1; /* node-global public queue */

    if (torc_node_id() != target_node) {
#if DBG
        printf("enqueuing remotely: rte = %p\n", (void *)rte);
#endif
        send_descriptor(target_node, rte, TORC_NORMAL_ENQUEUE);
        _torc_put_reused_desc(rte);
    }
    else {
#if DBG
        printf("enqueuing locally: rte = %p\n", (void *)rte);
#endif
        read_arguments(rte);
        torc_to_i_rq(rte);
    }
}


void torc_to_rq_end__ (torc_t *rte)    /* node version */
{
    int target_node;
    int total_nodes = torc_num_nodes();

    if (total_nodes <= 0)
        Error("invalid total node count");

    pthread_mutex_lock(&rq_end_node_lock);

    if (!rq_end_node_initialized) {
        rq_end_next_node = torc_node_id();
        rq_end_node_initialized = 1;
    }

    target_node = rq_end_next_node;
    rq_end_next_node = (rq_end_next_node + 1) % total_nodes;

    pthread_mutex_unlock(&rq_end_node_lock);

#if DBG
    printf("rte_to_rq_end: target_node = %d\n", target_node);
#endif

    rte->insert_private = 0;
    rte->inter_node = 1;
    rte->target_queue = -1;    /* global */
    if (torc_node_id() != target_node) {
#if DBG
        printf("enqueing remotely: rte = %p\n", (void *)rte);
#endif
        send_descriptor(target_node, rte, TORC_NORMAL_ENQUEUE);
        _torc_put_reused_desc(rte);
    }
    else {
#if DBG
        printf("enqueing locally: rte = %p\n", (void *)rte);
#endif
        /* read the arguments */
        for (int i = 0; i < rte->narg; i++)
            rte->temparg[i] = rte->localarg[i];

        torc_to_i_rq_end(rte);
    }
}

/* worker version */
void torc_to_rq_end (torc_t *rte)
{
    int target_worker;
    int total_workers;
    int target_node, target_queue;

    if (torc_num_nodes() == 1) {
        torc_to_i_rq_end(rte);
        return;
    }

    total_workers = torc_num_workers();
    if (total_workers <= 0)
        Error("invalid total worker count");

    pthread_mutex_lock(&scheduling_lock);

    if (!target_worker_initialized) {
        next_target_worker = torc_worker_id();
        target_worker_initialized = 1;
    }

    target_worker = next_target_worker;
    next_target_worker = (next_target_worker + 1) % total_workers;

    pthread_mutex_unlock(&scheduling_lock);

    target_node  = global_thread_id_to_node_id(target_worker);
    target_queue = global_thread_id_to_local_thread_id(target_worker);

#if DBG
    printf("rte_to_rq_end: target_worker = %d, target_node = %d, target_queue = %d\n",
           target_worker, target_node, target_queue);
#endif

    rte->insert_private = 0;
    rte->inter_node = 1;
    rte->target_queue = target_queue;    /* global */
    if (torc_node_id() != target_node) {
#if DBG
        printf("enqueing remotely: rte = %p\n", (void *)rte);
#endif
        send_descriptor(target_node, rte, TORC_NORMAL_ENQUEUE);
        _torc_put_reused_desc(rte);
    }
    else {
#if DBG
        printf("enqueing locally: rte = %p\n", (void *)rte);
#endif
        /* read the arguments */
        for (int i = 0; i < rte->narg; i++)
            rte->temparg[i] = rte->localarg[i];

        torc_to_i_rq_end(rte);
    }
}


/*
 * Public local (worker) queue
 */

void torc_to_lrq_end (int target, torc_t *rte)
{
    int target_node, target_queue;

    if (torc_num_nodes() == 1) {
        torc_to_i_rq_end(rte);
        return;
    }

    target_node  = global_thread_id_to_node_id(target);
    target_queue = global_thread_id_to_local_thread_id(target);

#if DBG
    printf("rte_to_lrq_end: target = %d, target_node = %d, target_queue = %d\n", target, target_node, target_queue);
#endif
    rte->inter_node = 1;
    rte->target_queue = target_queue;
    if (torc_node_id() != target_node) {
#if DBG
        printf("enqueing remotely: rte = %p\n", (void *)rte);
#endif
        send_descriptor(target_node, rte, TORC_NORMAL_ENQUEUE);
        _torc_put_reused_desc(rte);
    }
    else {
#if DBG
        printf("enqueing locally: rte = %p\n", (void *)rte);
#endif
        read_arguments(rte);
        torc_to_i_rq_end(rte);
    }
}


void torc_to_lrq (int target, torc_t *rte)
{
    int target_node, target_queue;

    if (torc_num_nodes() == 1) {
        torc_to_i_rq(rte);
        return;
    }

    target_node  = global_thread_id_to_node_id(target);
    target_queue = global_thread_id_to_local_thread_id(target);

#if DBG
    printf("rte_to_lrq_end: target = %d, target_node = %d, target_queue = %d\n", target, target_node, target_queue);
#endif
    rte->inter_node = 1;
    rte->target_queue = target_queue;
    rte->insert_in_front = 1;
    if (torc_node_id() != target_node) {
#if DBG
        printf("enqueing remotely: rte = %p\n", (void *)rte);
#endif
        send_descriptor(target_node, rte, TORC_NORMAL_ENQUEUE);
        _torc_put_reused_desc(rte);
    }
    else {
#if DBG
        printf("enqueing locally: rte = %p\n", (void *)rte);
#endif
        read_arguments(rte);
        torc_to_i_rq(rte);
    }
}


/*
 * Private global queue
 */

void torc_to_npq (int target_node, torc_t *rte)
{
#if DBG
    printf("rte_to_prq_2: target = %d, target_node = %d, target_queue = %s\n", -1, target_node, "intra-node gq");
#endif

    rte->insert_private = 1;
    rte->insert_in_front = 1;
    rte->inter_node = 1;    /* alternatively, it can be set only when it goes outside */
    /*    if (rte->target_queue < 0)*/
    rte->target_queue = -1;
    if (torc_node_id() != target_node) {
#if DBG
        printf("enqueing remotely: rte = %p\n", (void *)rte);
#endif
        send_descriptor(target_node, rte, TORC_NORMAL_ENQUEUE);
        _torc_put_reused_desc(rte);
    }
    else {
#if DBG
        printf("enqueing locally: rte = %p\n", (void *)rte);
#endif
        read_arguments(rte);
        torc_to_i_pq(rte);
    }
}

void torc_to_npq_end (int target_node, torc_t *rte)
{
#if DBG
    printf("rte_to_prq_end_2: target = %d, target_node = %d, target_queue = %s\n", -1, target_node, "intra-node gq");
#endif

    rte->insert_private = 1;
    rte->inter_node = 1;    /* alternatively, it can be set only when it goes outside */
    /*    if (rte->target_queue < 0)*/
    rte->target_queue = -1;
    if (torc_node_id() != target_node) {
#if DBG
        printf("enqueing remotely: rte = %p\n", (void *)rte);
#endif
        send_descriptor(target_node, rte, TORC_NORMAL_ENQUEUE);
        _torc_put_reused_desc(rte);
    }
    else {
#if DBG
        printf("enqueing locally: rte = %p\n", (void *)rte);
#endif
        read_arguments(rte);
        torc_to_i_pq_end(rte);
    }
}



void torc_to_prq (int target, torc_t *rte)
{
    int target_node = global_thread_id_to_node_id(target);

    torc_to_npq (target_node, rte);
}
