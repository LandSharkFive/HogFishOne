using System.Collections;

namespace HogFishOne
{
    public static class Util
    {
        private static readonly Random rnd = new Random();

        /// <summary>
        /// Performs an in-place Fisher-Yates shuffle to randomize the order of elements.
        /// </summary>
        /// <remarks>
        /// Used primarily by stress tests to ensure that B+ Tree insertions and deletions 
        /// occur in a non-sequential order, forcing complex node splits and rebalancing.
        /// </remarks>
        public static void Shuffle(List<int> list)
        {
            int n = list.Count;
            while (n > 1)
            {
                n--;
                int k = rnd.Next(n + 1);
                int value = list[k];
                list[k] = list[n];
                list[n] = value;
            }
        }

        /// <summary>
        /// Shuffle a list of any type using the Fisher-Yates algorithm.
        /// </summary>
        public static void ShuffleList<T>(List<T> list)
        {
            int n = list.Count;
            while (n > 1)
            {
                n--;
                int k = rnd.Next(n + 1);
                T value = list[k];
                list[k] = list[n];
                list[n] = value;
            }
        }

        /// <summary>
        /// Print a list.
        /// </summary>
        public static void PrintList(List<int> list)
        {
            foreach (int[] chunk in list.Chunk(20))
            {
                Console.WriteLine(string.Join(" ", chunk));
            }
            Console.WriteLine();
        }

        /// <summary>
        /// Optimized check to verify that a list of integers is in non-descending order.
        /// </summary>
        /// <remarks>
        /// Used as a high-speed pre-condition check for operations requiring ordered keys, 
        /// such as bulk loading or validating sequence-generated IDs.
        /// </remarks>
        public static bool IsSorted(List<int> a)
        {
            for (int i = 1; i < a.Count; i++)
            {
                if (a[i - 1] > a[i])
                {
                    return false;
                }
            }
            return true;
        }

        /// <summary>
        /// Verifies that a list is in non-descending order based on the provided or default comparer.
        /// </summary>
        /// <remarks>
        /// Critical for BulkLoad operations, where unsorted input would violate B+ Tree 
        /// invariants and lead to structural corruption.
        /// </remarks>
        public static bool IsSortedList<T>(List<T> list, IComparer<T>? comparer = default)
        {
            if (list.Count <= 1)
                return true;

            comparer ??= Comparer<T>.Default;

            for (var i = 1; i < list.Count; i++)
            {
                if (comparer.Compare(list[i - 1], list[i]) > 0)
                    return false;
            }

            return true;
        }

        /// <summary>
        /// Does the list have any duplicates?
        /// </summary>
        public static bool HasDuplicate(List<int> source)
        {
            var set = new HashSet<int>();
            foreach (var item in source)
            {
                if (!set.Add(item))
                    return true;
            }
            return false;
        }

        /// <summary>
        /// Generates an enumerate of prime numbers up to a specified maximum.
        /// </summary>
        public static IEnumerable<int> GetPrimes(int max)
        {
            if (max < 0) yield break; // No primes for negative numbers
            const int MaxAllowed = 1000000;
            if (max > MaxAllowed) 
                throw new ArgumentOutOfRangeException(nameof(max), $"Must be less than {MaxAllowed}.");

            var isPrime = new BitArray(max + 1, true); // Initialize all to true

            // 0 and 1 are not primes
            isPrime[0] = false;
            isPrime[1] = false;

            // Sieve logic: for each number starting from 2
            int limit = (int)Math.Sqrt(max);
            for (var i = 2; i <= limit; i++)
            {
                if (!isPrime[i]) continue; // Skip if already marked as composite

                // Mark all multiples of i (starting from i*i) as composite
                for (var j = i * i; j <= max; j += i)
                {
                    isPrime[j] = false;
                }
            }

            // Yield all numbers still marked as prime
            for (var i = 2; i <= max; i++)
            {
                if (isPrime[i]) yield return i;
            }
        }

        /// <summary>
        /// Get a list of prime numbers.
        /// </summary>
        public static List<int> GetPrimesList(int max)
        {
            var results = new List<int>();
            foreach (var p in GetPrimes(max))
            {
                results.Add(p);
            }
            return results;
        }

        /// <summary>
        /// Write prime numbers to a text file.
        /// </summary>
        public static void WritePrimesToFile(string path, int max)
        {
            // 1. Pre-cleanup.
            File.Delete(path);

            // 2. Increase the Buffer Size.
            const int BufferSize = 65536;

            // We pass the buffer size into the StreamWriter constructor.
            using (var stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, BufferSize))
            using (var writer = new StreamWriter(stream, System.Text.Encoding.UTF8, BufferSize))
            {
                int index = 1;
                foreach (var prime in GetPrimes(max))
                {
                    writer.WriteLine($"{index}, {prime}");
                    index++;
                }
            }
        }


    }
}
