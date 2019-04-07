---
layout:      post
title:       "Spark Executor内存管理部分代码走读"
date:        2019-04-07 12:00:00
author:      "wayblink"
header-img:  "img/spark-banner.png"
header-mask: 0.3
catalog:     true
tags:
    - Spark
    - Olap
---

## 前言

本文因为是代码走读，因此组织结构不是很清晰，而是顺着看代码的流程一步一步，梳理了一遍Spark Executor内存管理的整体框架。结合代码看较好。

## spark.executor.memory和spark.executor.memoryOverhead

Spark on yarn 的executor内存大体分成两部分：EXECUTOR_MEMORY 和 MEMORY_OVERHEAD（额外内存），EXECUTOR_MEMORY就是spark程序运行的JVM最大堆内存，而MEMORY_OVERHEAD是预留给堆外内存和其他系统额外开销的内存（Native方法调用，线程栈， NIO Buffer等开销），应有：
```EXECUTOR_MEMORY + MEMORY_OVERHEAD <= YARN_CONTAINER_MEMORY```

EXECUTOR_MEMORY和MEMORY_OVERHEAD分别由两个参数进行控制，即：
- spark.executor.memory
- spark.executor.memoryOverhead

在源码YarnAllocator中可以看到从两个配置读取得到executorMemory和memoryOverhead后相加，作为向yarn申请的内存量，可以看到，memoryOverhead的计算稍微复杂，还要通过几个参数取max得到，这是因为memoryOverhead不能设为0，要留有系统规定的一定大小，保证程序顺利进行。


YarnAllocator
```
// Executor memory in MB.
protected val executorMemory = sparkConf.get(EXECUTOR_MEMORY).toInt
// Additional memory overhead.
protected val memoryOverhead: Int = sparkConf.get(EXECUTOR_MEMORY_OVERHEAD).getOrElse(
  math.max((MEMORY_OVERHEAD_FACTOR * executorMemory).toInt, MEMORY_OVERHEAD_MIN)).toInt
// Number of cores per executor.
protected val executorCores = sparkConf.get(EXECUTOR_CORES)
// Resource capability requested for each executors
private[yarn] val resource = Resource.newInstance(executorMemory + memoryOverhead, executorCores)

//resource用于提交创建container申请
createContainerRequest(resource, nodes, racks)
```


EXECUTOR_MEMORY会在生成spark启动命令时转化成JVM的-Xmx参数，也就是说，spark.executor.memory等价于executor的JVM堆内存。

SparkClassCommandbuilder
```
switch (className)
  ...... 
  case "org.apache.spark.executor.CoarseGrainedExecutorBackend":
    javaOptsKeys.add("SPARK_EXECUTOR_OPTS");
    memKey = "SPARK_EXECUTOR_MEMORY";
    extraClassPath = getenv("SPARK_EXECUTOR_CLASSPATH");
    break;
  ......

  String mem = firstNonEmpty(memKey != null ? System.getenv(memKey) : null, DEFAULT_MEM);
  cmd.add("-Xmx" + mem);
  cmd.add(className);
  md.addAll(classArgs);
  return cmd;
```

## spark.memory.offHeap.enabled和spark.memory.offHeap.size

两者是关于Off-heap内存（堆外内存）使用的参数，堆外内存占用的空间就是上一节说到的MEMORY_OVERHEAD。理论上在Spark使用大堆内内存时其 GC 机制容易影响性能；堆外内存相比于堆内存使用复杂，但精确的内存控制使其更高效。在 Spark 中，很多地方会有大数组大内存的需求，高效的内存使用时必须的，因此 Spark 也提供了堆外内存的支持，以优化 Application 运行性能。目前我们的设置是关闭的。

## spark.memory.fraction和spark.memory.storageFraction

上文的EXECUTOR_MEMORY规定了一个executor最大的JVM堆内存，而这块内存具体怎么用还需要更详细的设置。Spark将内存和存储运行内存和其他内存，存储运行内存是指spark core程序运行所需要的内存，其他内存是指用户定义的数据结构和spark的元数据。spark.memory.fraction即设置存储运行内存占EXECUTOR_MEMORY的比例，默认0.6。

存储运行内存又分为存储（storage）和运行（execution）两部分：
- execution 内存：用于 shuffles，如joins、sorts 和 aggregations，避免频繁的 IO 而需要内存 buffer
- storage 内存：用于 caching RDD，缓存 broadcast 数据及缓存 task results

在老版本的Spark（可以设置spark.memory.useLegacyMode=true切换至老模式，默认为false），这两部分需要分别设置调优，在1.6之后两块变成了统一可相互占用的模式，在execution内存未使用的时候，storage可以占用所有剩余内存，反之亦然。当内存用满时，execution可以驱逐storage内存，但storage不能驱逐execution内存，spark.memory.storageFraction规定的是storage被驱逐的底线的比例，默认是0.5，即当storage被execution驱逐时，至少可以保留0.5的空间，无会被驱逐得低于0.5。

## UnifiedMemoryManager

MemoryManager是用于管理executor中内存使用情况的基类，两个实现分别是UnifiedMemoryManager和StaticMemoryManager，StaticMemoryManager是spark 1.6之前实现（设置spark.memory.useLegacyMode=true可以切换），现在只用关注UnifiedMemoryManager。

一个Executor有且只有一个UnifiedMemoryManager对象。上文说到的spark.memory.fraction和spark.memory.storageFraction就是在这里生效的：
- MaxMemory =（系统内存（JVM heap）-预留内存300M）* spark.memory.fraction


```
/**
 * Return the total amount of memory shared between execution and storage, in bytes.
 */
private def getMaxMemory(conf: SparkConf): Long = {
  val systemMemory = conf.getLong("spark.testing.memory", Runtime.getRuntime.maxMemory)
  val reservedMemory = conf.getLong("spark.testing.reservedMemory",
    if (conf.contains("spark.testing")) 0 else RESERVED_SYSTEM_MEMORY_BYTES)
  val minSystemMemory = (reservedMemory * 1.5).ceil.toLong
  if (systemMemory < minSystemMemory) {
    throw new IllegalArgumentException(s"System memory $systemMemory must " +
      s"be at least $minSystemMemory. Please increase heap size using the --driver-memory " +
      s"option or spark.driver.memory in Spark configuration.")
  }
  // SPARK-12759 Check executor memory to fail fast if memory is insufficient
  if (conf.contains("spark.executor.memory")) {
    val executorMemory = conf.getSizeAsBytes("spark.executor.memory")
    if (executorMemory < minSystemMemory) {
      throw new IllegalArgumentException(s"Executor memory $executorMemory must be at least " +
        s"$minSystemMemory. Please increase executor memory using the " +
        s"--executor-memory option or spark.executor.memory in Spark configuration.")
    }
  }
  val usableMemory = systemMemory - reservedMemory
  val memoryFraction = conf.getDouble("spark.memory.fraction", 0.6)
  (usableMemory * memoryFraction).toLong
}
```

然后，这个MaxMemory准确说是最大堆内可用内存，它用于初始化UnifiedMemoryManager。

```
def apply(conf: SparkConf, numCores: Int): UnifiedMemoryManager = {
  val maxMemory = getMaxMemory(conf)
  new UnifiedMemoryManager(
    conf,
    maxHeapMemory = maxMemory,
    onHeapStorageRegionSize =
      (maxMemory * conf.getDouble("spark.memory.storageFraction", 0.5)).toLong,
    numCores = numCores)
}
```

UnifiedMemoryManager的核心方法是申请和释放空间
- acquireStorageMemory
- acquireExecutionMemory
- releaseExecutionMemory
- releaseStorageMemory

以acquireStorageMemory为例，传入了blockId，长度，和存储模式，返回一个Boolean值，即是否允许申请这么多的内存。

```
override def acquireStorageMemory(
    blockId: BlockId,
    numBytes: Long,
    memoryMode: MemoryMode): Boolean = synchronized {
  assertInvariants()
  assert(numBytes >= 0)
  val (executionPool, storagePool, maxMemory) = memoryMode match {
    case MemoryMode.ON_HEAP => (
      onHeapExecutionMemoryPool,
      onHeapStorageMemoryPool,
      maxOnHeapStorageMemory)
    case MemoryMode.OFF_HEAP => (
      offHeapExecutionMemoryPool,
      offHeapStorageMemoryPool,
      maxOffHeapStorageMemory)
  }
  if (numBytes > maxMemory) {
    // Fail fast if the block simply won't fit
    logInfo(s"Will not store $blockId as the required space ($numBytes bytes) exceeds our " +
      s"memory limit ($maxMemory bytes)")
    return false
  }
  if (numBytes > storagePool.memoryFree) {
    // There is not enough free memory in the storage pool, so try to borrow free memory from
    // the execution pool.
    //这里有从executionPool借空间的操作
    val memoryBorrowedFromExecution = Math.min(executionPool.memoryFree,
      numBytes - storagePool.memoryFree)
    executionPool.decrementPoolSize(memoryBorrowedFromExecution)
    storagePool.incrementPoolSize(memoryBorrowedFromExecution)
  }
  storagePool.acquireMemory(blockId, numBytes)
}
```

上面代码引入两个新东西：MemoryMode和MemoryPool，MemoryMode就是ON-HEAP和OFF-HEAP两种存储模式，MemoryPool是管理某一类内存的类。Spark任务运行的内存按照存储模式分为ON-HEAP和OFF-HEAP两部分，按照用途分为Storage和Execution两部分，因此总共分为四种类型，对应四种MemoryPool。MemoryManager封装了四个MemoryPool对象，上层的acquire和release操作会根据MemoryMode转化成对具体MemoryPool的操作。

```
/**
 * An abstract memory manager that enforces how memory is shared between execution and storage.
 *
 * In this context, execution memory refers to that used for computation in shuffles, joins,
 * sorts and aggregations, while storage memory refers to that used for caching and propagating
 * internal data across the cluster. There exists one MemoryManager per JVM.
 */
private[spark] abstract class MemoryManager(
    conf: SparkConf,
    numCores: Int,
    onHeapStorageMemory: Long,
    onHeapExecutionMemory: Long) extends Logging {

  // -- Methods related to memory allocation policies and bookkeeping ------------------------------

  @GuardedBy("this")
  protected val onHeapStorageMemoryPool = new StorageMemoryPool(this, MemoryMode.ON_HEAP)
  @GuardedBy("this")
  protected val offHeapStorageMemoryPool = new StorageMemoryPool(this, MemoryMode.OFF_HEAP)
  @GuardedBy("this")
  protected val onHeapExecutionMemoryPool = new ExecutionMemoryPool(this, MemoryMode.ON_HEAP)
  @GuardedBy("this")
  protected val offHeapExecutionMemoryPool = new ExecutionMemoryPool(this, MemoryMode.OFF_HEAP)

  onHeapStorageMemoryPool.incrementPoolSize(onHeapStorageMemory)
  onHeapExecutionMemoryPool.incrementPoolSize(onHeapExecutionMemory)

  protected[this] val maxOffHeapMemory = conf.get(MEMORY_OFFHEAP_SIZE)
  protected[this] val offHeapStorageMemory =
    (maxOffHeapMemory * conf.getDouble("spark.memory.storageFraction", 0.5)).toLong

  offHeapExecutionMemoryPool.incrementPoolSize(maxOffHeapMemory - offHeapStorageMemory)
  offHeapStorageMemoryPool.incrementPoolSize(offHeapStorageMemory)
```

UnifiedMemoryManager还有一个重要方法是tungstenMemoryAllocator，跟据MemoryMode选择MemoryAllocator，ON_HEAP对应HeapMemoryAllocator，OFF_HEAP对应UnsafeMemoryAllocator。

```
/**
 * Allocates memory for use by Unsafe/Tungsten code.
 */
private[memory] final val tungstenMemoryAllocator: MemoryAllocator = {
  tungstenMemoryMode match {
    case MemoryMode.ON_HEAP => MemoryAllocator.HEAP
    case MemoryMode.OFF_HEAP => MemoryAllocator.UNSAFE
  }
}
```

## TaskMemoryManager

TaskMemoryManager是管理一个spark task的内存的类，从构造起来看，需要指定tungstenMemoryMode，memoryManager，taskAttemptId和consumers。

```
/**
 * Construct a new TaskMemoryManager.
 */
public TaskMemoryManager(MemoryManager memoryManager, long taskAttemptId) {
  this.tungstenMemoryMode = memoryManager.tungstenMemoryMode();
  this.memoryManager = memoryManager;
  this.taskAttemptId = taskAttemptId;
  this.consumers = new HashSet<>();
}
```

- tungstenMemoryMode即ON-HEAP/OFF-HEAP的选项
- memoryManager就是上面说到的MemoryManager类对象。要注意，TaskMemoryManager并不是MemoryManager的实现。上面说过UnifiedMemoryManager是executor唯一或者说JVM唯一的，所以UnifiedMemoryManager与TaskMemoryManager是一对多关系，TaskMemoryManager向UnifiedMemoryManager申请和释放executor的内存并真实地分配它们。
- taskAttemptId就是task的id
- consumers是一个Set，存储的是这个task用到的MemoryConsumer，下面会讲

TaskMemoryManager要解决的一个关键问题是堆内内存和堆外内存的统一，堆外内存使用的是64bit的long代表地址，而Java堆内内存对象是用一个对象引用和一个64bit偏移量代表的，这里的处理是把堆内对象转换成一个由13bit page_number和51bit offset拼成的64bit long代表。具体细节不展开，总之现在堆内堆外内存统一，都可以用一个64bit long来代表，并将内存分成了2^13=8192个page。创建了一个MemoryBlock类来抽象一个page。

MemoryBlock

```
/**
 * A consecutive block of memory, starting at a {@link MemoryLocation} with a fixed size.
 */
public class MemoryBlock extends MemoryLocation {
MemoryBlock代表spark内存中的一块连续内存。

TaskMemoryManager用一个MemoryBlock数组对象pageTable存储这个信息，并用13bit的Bitset来标示page是否被使用。
public class TaskMemoryManager {
  /** The number of bits used to address the page table. */
  private static final int PAGE_NUMBER_BITS = 13;

  /** The number of entries in the page table. */
  private static final int PAGE_TABLE_SIZE = 1 << PAGE_NUMBER_BITS;

  private final MemoryBlock[] pageTable = new MemoryBlock[PAGE_TABLE_SIZE];

  /** Bitmap for tracking free pages. */
  private final BitSet allocatedPages = new BitSet(PAGE_TABLE_SIZE);
```

MemoryConsumer

```
/**
 * A memory consumer of {@link TaskMemoryManager} that supports spilling.
 *
 * Note: this only supports allocation / spilling of Tungsten memory.
 */
public abstract class MemoryConsumer {

  protected final TaskMemoryManager taskMemoryManager;
  private final long pageSize;
  private final MemoryMode mode;
  protected long used;

  protected MemoryConsumer(TaskMemoryManager taskMemoryManager, long pageSize, MemoryMode mode) {
    this.taskMemoryManager = taskMemoryManager;
    this.pageSize = pageSize;
    this.mode = mode;
  }
```

顾名思义MemoryConsumer就是内存的消费者，调用MemoryConsumer.allocatePage()， MemoryConsumer.acquireMemory()等方法从TaskMemoryManager中申请内存。MemoryConsumer是个抽象类，而他的集体实现会发现都是Spark core和Spark SQL的算子。

              

TaskMemoryManager实际分配方法allocatePage

```
/**
 * Allocate a block of memory that will be tracked in the MemoryManager's page table; this is
 * intended for allocating large blocks of Tungsten memory that will be shared between operators.
 *
 * Returns `null` if there was not enough memory to allocate the page. May return a page that
 * contains fewer bytes than requested, so callers should verify the size of returned pages.
 *
 * @throws TooLargePageException
 */
public MemoryBlock allocatePage(long size, MemoryConsumer consumer) {
  assert(consumer != null);
  assert(consumer.getMode() == tungstenMemoryMode);
  if (size > MAXIMUM_PAGE_SIZE_BYTES) {
    throw new TooLargePageException(size);
  }

  long acquired = acquireExecutionMemory(size, consumer);
  if (acquired <= 0) {
    return null;
  }

  final int pageNumber;
  synchronized (this) {
    pageNumber = allocatedPages.nextClearBit(0);
    if (pageNumber >= PAGE_TABLE_SIZE) {
      releaseExecutionMemory(acquired, consumer);
      throw new IllegalStateException(
        "Have already allocated a maximum of " + PAGE_TABLE_SIZE + " pages");
    }
    allocatedPages.set(pageNumber);
  }
  MemoryBlock page = null;
  try {
    page = memoryManager.tungstenMemoryAllocator().allocate(acquired);
  } catch (OutOfMemoryError e) {
    logger.warn("Failed to allocate a page ({} bytes), try again.", acquired);
    // there is no enough memory actually, it means the actual free memory is smaller than
    // MemoryManager thought, we should keep the acquired memory.
    synchronized (this) {
      acquiredButNotUsed += acquired;
      allocatedPages.clear(pageNumber);
    }
    // this could trigger spilling to free some pages.
    return allocatePage(size, consumer);
  }
  page.pageNumber = pageNumber;
  pageTable[pageNumber] = page;
  if (logger.isTraceEnabled()) {
    logger.trace("Allocate page number {} ({} bytes)", pageNumber, acquired);
  }
  return page;
}
```

从代码里看到，关键点是调用acquireExecutionMemory获取资源，然后调用memoryManager.tungstenMemoryAllocator().allocate(acquired)分配资源，并进行pageTable的管理。


## 总结
到这里整个Executor内存的调用关系大致清晰：一个spark任务的task发送到executor执行，task就是若干个算子操作，这些会用到内存的操作都会直接或间接继承MemoryConsumer，管理本算子的内存消费。每个task有唯一的TaskMemoryManager，task的所有MemoryConsumer在TaskMemoryManager初始化时传入，并在程序运行时与TaskMemoryManager交互，TaskMemoryManager将来自MemoryConsumer的请求转化成对Executor的UnifiedMemoryManager的请求，并根据返回结果，真实地调用MemoryAllocator方法操作内存。


## 参考

https://www.jianshu.com/p/8f9ed2d58a26
https://www.jianshu.com/p/2e9eda28e86c
https://www.jianshu.com/p/10e91ace3378
https://github.com/ColZer/DigAndBuried/blob/master/spark/spark-memory-manager.md
