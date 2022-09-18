#ifndef EPOCH_H
#define EPOCH_H

#include <atomic>

namespace taas
{
    class EpochManager
    {
    private:
        static EpochManager* em_;

        int epoch_duration_; // ms
        std::atomic<int32_t> cur_epoch_;
        bool deconstructor_invoked_;

        EpochManager(int epoch_duration);
        ~EpochManager();
        EpochManager(const EpochManager&);
        EpochManager& operator=(const EpochManager&);
        void Run();
    public:
        static EpochManager* GetInstance();
        int GetPhysicalEpoch();
    };
} // namespace taas



#endif