/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { NzI18nModule } from '../i18n/nz-i18n.module';
import { NzIconModule } from '../icon/nz-icon.module';
import { NzToolTipModule } from '../tooltip/nz-tooltip.module';
import { NzProgressModule } from './../progress/nz-progress.module';
import { NzUploadBtnComponent } from './nz-upload-btn.component';
import { NzUploadListComponent } from './nz-upload-list.component';
import { NzUploadComponent } from './nz-upload.component';
var NzUploadModule = /** @class */ (function () {
    function NzUploadModule() {
    }
    NzUploadModule.decorators = [
        { type: NgModule, args: [{
                    imports: [CommonModule, FormsModule, NzToolTipModule, NzProgressModule, NzI18nModule, NzIconModule],
                    declarations: [NzUploadComponent, NzUploadBtnComponent, NzUploadListComponent],
                    exports: [NzUploadComponent]
                },] }
    ];
    return NzUploadModule;
}());
export { NzUploadModule };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotdXBsb2FkLm1vZHVsZS5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJ1cGxvYWQvbnotdXBsb2FkLm1vZHVsZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLFlBQVksRUFBRSxNQUFNLGlCQUFpQixDQUFDO0FBQy9DLE9BQU8sRUFBRSxRQUFRLEVBQUUsTUFBTSxlQUFlLENBQUM7QUFDekMsT0FBTyxFQUFFLFdBQVcsRUFBRSxNQUFNLGdCQUFnQixDQUFDO0FBRTdDLE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSx3QkFBd0IsQ0FBQztBQUN0RCxPQUFPLEVBQUUsWUFBWSxFQUFFLE1BQU0sd0JBQXdCLENBQUM7QUFDdEQsT0FBTyxFQUFFLGVBQWUsRUFBRSxNQUFNLDhCQUE4QixDQUFDO0FBRS9ELE9BQU8sRUFBRSxnQkFBZ0IsRUFBRSxNQUFNLGtDQUFrQyxDQUFDO0FBQ3BFLE9BQU8sRUFBRSxvQkFBb0IsRUFBRSxNQUFNLDJCQUEyQixDQUFDO0FBQ2pFLE9BQU8sRUFBRSxxQkFBcUIsRUFBRSxNQUFNLDRCQUE0QixDQUFDO0FBQ25FLE9BQU8sRUFBRSxpQkFBaUIsRUFBRSxNQUFNLHVCQUF1QixDQUFDO0FBRTFEO0lBQUE7SUFLNkIsQ0FBQzs7Z0JBTDdCLFFBQVEsU0FBQztvQkFDUixPQUFPLEVBQUUsQ0FBQyxZQUFZLEVBQUUsV0FBVyxFQUFFLGVBQWUsRUFBRSxnQkFBZ0IsRUFBRSxZQUFZLEVBQUUsWUFBWSxDQUFDO29CQUNuRyxZQUFZLEVBQUUsQ0FBQyxpQkFBaUIsRUFBRSxvQkFBb0IsRUFBRSxxQkFBcUIsQ0FBQztvQkFDOUUsT0FBTyxFQUFFLENBQUMsaUJBQWlCLENBQUM7aUJBQzdCOztJQUM0QixxQkFBQztDQUFBLEFBTDlCLElBSzhCO1NBQWpCLGNBQWMiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBDb21tb25Nb2R1bGUgfSBmcm9tICdAYW5ndWxhci9jb21tb24nO1xuaW1wb3J0IHsgTmdNb2R1bGUgfSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7IEZvcm1zTW9kdWxlIH0gZnJvbSAnQGFuZ3VsYXIvZm9ybXMnO1xuXG5pbXBvcnQgeyBOekkxOG5Nb2R1bGUgfSBmcm9tICcuLi9pMThuL256LWkxOG4ubW9kdWxlJztcbmltcG9ydCB7IE56SWNvbk1vZHVsZSB9IGZyb20gJy4uL2ljb24vbnotaWNvbi5tb2R1bGUnO1xuaW1wb3J0IHsgTnpUb29sVGlwTW9kdWxlIH0gZnJvbSAnLi4vdG9vbHRpcC9uei10b29sdGlwLm1vZHVsZSc7XG5cbmltcG9ydCB7IE56UHJvZ3Jlc3NNb2R1bGUgfSBmcm9tICcuLy4uL3Byb2dyZXNzL256LXByb2dyZXNzLm1vZHVsZSc7XG5pbXBvcnQgeyBOelVwbG9hZEJ0bkNvbXBvbmVudCB9IGZyb20gJy4vbnotdXBsb2FkLWJ0bi5jb21wb25lbnQnO1xuaW1wb3J0IHsgTnpVcGxvYWRMaXN0Q29tcG9uZW50IH0gZnJvbSAnLi9uei11cGxvYWQtbGlzdC5jb21wb25lbnQnO1xuaW1wb3J0IHsgTnpVcGxvYWRDb21wb25lbnQgfSBmcm9tICcuL256LXVwbG9hZC5jb21wb25lbnQnO1xuXG5ATmdNb2R1bGUoe1xuICBpbXBvcnRzOiBbQ29tbW9uTW9kdWxlLCBGb3Jtc01vZHVsZSwgTnpUb29sVGlwTW9kdWxlLCBOelByb2dyZXNzTW9kdWxlLCBOekkxOG5Nb2R1bGUsIE56SWNvbk1vZHVsZV0sXG4gIGRlY2xhcmF0aW9uczogW056VXBsb2FkQ29tcG9uZW50LCBOelVwbG9hZEJ0bkNvbXBvbmVudCwgTnpVcGxvYWRMaXN0Q29tcG9uZW50XSxcbiAgZXhwb3J0czogW056VXBsb2FkQ29tcG9uZW50XVxufSlcbmV4cG9ydCBjbGFzcyBOelVwbG9hZE1vZHVsZSB7fVxuIl19