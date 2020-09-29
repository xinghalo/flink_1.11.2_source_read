/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { NzToolTipModule } from '../tooltip/nz-tooltip.module';
import { NzSliderHandleComponent } from './nz-slider-handle.component';
import { NzSliderMarksComponent } from './nz-slider-marks.component';
import { NzSliderStepComponent } from './nz-slider-step.component';
import { NzSliderTrackComponent } from './nz-slider-track.component';
import { NzSliderComponent } from './nz-slider.component';
var NzSliderModule = /** @class */ (function () {
    function NzSliderModule() {
    }
    NzSliderModule.decorators = [
        { type: NgModule, args: [{
                    exports: [
                        NzSliderComponent,
                        NzSliderTrackComponent,
                        NzSliderHandleComponent,
                        NzSliderStepComponent,
                        NzSliderMarksComponent
                    ],
                    declarations: [
                        NzSliderComponent,
                        NzSliderTrackComponent,
                        NzSliderHandleComponent,
                        NzSliderStepComponent,
                        NzSliderMarksComponent
                    ],
                    imports: [CommonModule, NzToolTipModule]
                },] }
    ];
    return NzSliderModule;
}());
export { NzSliderModule };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotc2xpZGVyLm1vZHVsZS5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJzbGlkZXIvbnotc2xpZGVyLm1vZHVsZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLFlBQVksRUFBRSxNQUFNLGlCQUFpQixDQUFDO0FBQy9DLE9BQU8sRUFBRSxRQUFRLEVBQUUsTUFBTSxlQUFlLENBQUM7QUFFekMsT0FBTyxFQUFFLGVBQWUsRUFBRSxNQUFNLDhCQUE4QixDQUFDO0FBRS9ELE9BQU8sRUFBRSx1QkFBdUIsRUFBRSxNQUFNLDhCQUE4QixDQUFDO0FBQ3ZFLE9BQU8sRUFBRSxzQkFBc0IsRUFBRSxNQUFNLDZCQUE2QixDQUFDO0FBQ3JFLE9BQU8sRUFBRSxxQkFBcUIsRUFBRSxNQUFNLDRCQUE0QixDQUFDO0FBQ25FLE9BQU8sRUFBRSxzQkFBc0IsRUFBRSxNQUFNLDZCQUE2QixDQUFDO0FBQ3JFLE9BQU8sRUFBRSxpQkFBaUIsRUFBRSxNQUFNLHVCQUF1QixDQUFDO0FBRTFEO0lBQUE7SUFpQjZCLENBQUM7O2dCQWpCN0IsUUFBUSxTQUFDO29CQUNSLE9BQU8sRUFBRTt3QkFDUCxpQkFBaUI7d0JBQ2pCLHNCQUFzQjt3QkFDdEIsdUJBQXVCO3dCQUN2QixxQkFBcUI7d0JBQ3JCLHNCQUFzQjtxQkFDdkI7b0JBQ0QsWUFBWSxFQUFFO3dCQUNaLGlCQUFpQjt3QkFDakIsc0JBQXNCO3dCQUN0Qix1QkFBdUI7d0JBQ3ZCLHFCQUFxQjt3QkFDckIsc0JBQXNCO3FCQUN2QjtvQkFDRCxPQUFPLEVBQUUsQ0FBQyxZQUFZLEVBQUUsZUFBZSxDQUFDO2lCQUN6Qzs7SUFDNEIscUJBQUM7Q0FBQSxBQWpCOUIsSUFpQjhCO1NBQWpCLGNBQWMiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBDb21tb25Nb2R1bGUgfSBmcm9tICdAYW5ndWxhci9jb21tb24nO1xuaW1wb3J0IHsgTmdNb2R1bGUgfSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuaW1wb3J0IHsgTnpUb29sVGlwTW9kdWxlIH0gZnJvbSAnLi4vdG9vbHRpcC9uei10b29sdGlwLm1vZHVsZSc7XG5cbmltcG9ydCB7IE56U2xpZGVySGFuZGxlQ29tcG9uZW50IH0gZnJvbSAnLi9uei1zbGlkZXItaGFuZGxlLmNvbXBvbmVudCc7XG5pbXBvcnQgeyBOelNsaWRlck1hcmtzQ29tcG9uZW50IH0gZnJvbSAnLi9uei1zbGlkZXItbWFya3MuY29tcG9uZW50JztcbmltcG9ydCB7IE56U2xpZGVyU3RlcENvbXBvbmVudCB9IGZyb20gJy4vbnotc2xpZGVyLXN0ZXAuY29tcG9uZW50JztcbmltcG9ydCB7IE56U2xpZGVyVHJhY2tDb21wb25lbnQgfSBmcm9tICcuL256LXNsaWRlci10cmFjay5jb21wb25lbnQnO1xuaW1wb3J0IHsgTnpTbGlkZXJDb21wb25lbnQgfSBmcm9tICcuL256LXNsaWRlci5jb21wb25lbnQnO1xuXG5ATmdNb2R1bGUoe1xuICBleHBvcnRzOiBbXG4gICAgTnpTbGlkZXJDb21wb25lbnQsXG4gICAgTnpTbGlkZXJUcmFja0NvbXBvbmVudCxcbiAgICBOelNsaWRlckhhbmRsZUNvbXBvbmVudCxcbiAgICBOelNsaWRlclN0ZXBDb21wb25lbnQsXG4gICAgTnpTbGlkZXJNYXJrc0NvbXBvbmVudFxuICBdLFxuICBkZWNsYXJhdGlvbnM6IFtcbiAgICBOelNsaWRlckNvbXBvbmVudCxcbiAgICBOelNsaWRlclRyYWNrQ29tcG9uZW50LFxuICAgIE56U2xpZGVySGFuZGxlQ29tcG9uZW50LFxuICAgIE56U2xpZGVyU3RlcENvbXBvbmVudCxcbiAgICBOelNsaWRlck1hcmtzQ29tcG9uZW50XG4gIF0sXG4gIGltcG9ydHM6IFtDb21tb25Nb2R1bGUsIE56VG9vbFRpcE1vZHVsZV1cbn0pXG5leHBvcnQgY2xhc3MgTnpTbGlkZXJNb2R1bGUge31cbiJdfQ==