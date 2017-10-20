abstract class MergeAlgo( val outputDir: String ) {
  def apply( partition: Partition ): Partition
}
